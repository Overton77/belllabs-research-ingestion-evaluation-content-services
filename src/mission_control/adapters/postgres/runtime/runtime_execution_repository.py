"""RLS-scoped persistence for runtime bindings, attempts and interventions on mission_control.

A runtime binding is admitted once as an immutable canonical ``execution_binding``; its
mutable submission status lives in support ``runtime_execution_binding`` (version CAS).
Runtime submission attempts are support observations. Interventions are canonical
``command`` rows with support idempotency (``runtime_intervention``); their receipt is a
canonical ``delivery_report``.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import TypeAdapter

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.recovery.runtime_execution_bindings import (
    RuntimeBindingConflict,
    RuntimeBindingReservation,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.graph_runtime.contracts import (
    GraphExecutionSubmission,
    InterventionReceipt,
    RuntimeExecutionAttempt,
    RuntimeExecutionBinding,
    RuntimeExecutionProjection,
    RuntimeIntervention,
)
from mission_control.domain.graph_runtime.identities import ExecutionEpochKey

INTERVENTION_ADAPTER: TypeAdapter[RuntimeIntervention] = TypeAdapter[RuntimeIntervention](
    RuntimeIntervention
)
BINDING_CONTRACT = "mc.runtime-execution-binding/1"
RECEIPT_SEMANTICS = "mc.runtime-intervention-receipt/1"
_REPORT_OUTCOME = {
    "accepted": "delivered",
    "existing": "delivered",
    "stale": "rejected",
    "rejected": "rejected",
    "reconciliation_required": "unknown",
}
_COMMAND_LIFECYCLE = {
    "accepted": "delivered",
    "existing": "delivered",
    "stale": "superseded",
    "rejected": "rejected",
    "reconciliation_required": "delivering",
}


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
            args = await mc.begin(connection, binding.epoch.request_scope)
            await mc.advisory_lock(connection, f"runtime-binding:{binding.epoch.canonical_key}")
            prior = await connection.fetchrow(
                f"""
                SELECT submission_digest, binding_payload
                FROM mission_control.runtime_execution_binding
                WHERE {SCOPE}
                  AND (
                    submission_key = $4
                    OR submission_idempotency_key = $5
                    OR (run_key = $6 AND execution_epoch = $7)
                  )
                FOR UPDATE
                """,
                *args,
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
            run = await mc.require_run(connection, args, binding.epoch.belllabs_run_id)
            manifest = stable_json_dump(binding)
            execution_binding_id = uuid7()
            await connection.execute(
                """
                INSERT INTO mission_control.execution_binding (
                    installation_id, application_id, tenant_id, execution_binding_id,
                    binding_key, revision_id, run_id, program_node_id, subordinate_id,
                    binding_contract, manifest, manifest_ref, manifest_digest,
                    admission_decision, admitted_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, $9::jsonb, NULL, $10,
                        'admitted', $11, $11, $12)
                """,
                *args,
                execution_binding_id,
                f"runtime-binding:{binding.binding_id}",
                run["revision_id"],
                run["run_id"],
                BINDING_CONTRACT,
                _dump(manifest),
                sha256_digest(manifest),
                binding.created_at,
                mc.WRITER_REF,
            )
            deployment = binding.deployment
            thread = binding.agent_thread
            await connection.execute(
                """
                INSERT INTO mission_control.runtime_execution_binding (
                    installation_id, application_id, tenant_id, runtime_execution_binding_id,
                    binding_key, execution_binding_id, run_key, execution_epoch, submission_key,
                    submission_idempotency_key, submission_digest, run_plan_digest,
                    graph_assembly_digest, state_schema_digest, runtime_provider,
                    deployment_endpoint_key, deployment_revision, deployment_key, assistant_key,
                    graph_key, agent_server_thread_key, status, active, version,
                    binding_payload, updated_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                        $17, $18, $19, $20, $21, $22, $23, $24, $25::jsonb, $26, $27, $28)
                """,
                *args,
                uuid7(),
                binding.binding_id,
                execution_binding_id,
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
                binding.updated_at,
                binding.created_at,
                mc.WRITER_REF,
            )
        return RuntimeBindingReservation(binding=binding, created=True)

    async def get_binding(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, epoch.request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT binding_payload FROM mission_control.runtime_execution_binding
                WHERE {SCOPE} AND run_key = $4 AND execution_epoch = $5
                """,
                *args,
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
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT binding_payload FROM mission_control.runtime_execution_binding
                WHERE {SCOPE} AND submission_key = $4
                """,
                *args,
                submission_id,
            )
        return RuntimeExecutionBinding.model_validate(_json(payload)) if payload else None

    async def append_attempt(
        self,
        attempt: RuntimeExecutionAttempt,
    ) -> RuntimeExecutionAttempt:
        scope = attempt.attempt_key.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(
                connection,
                f"runtime-attempt:{scope}:{attempt.binding_id}:"
                f"{attempt.attempt_key.runtime_attempt}",
            )
            prior = await connection.fetchrow(
                f"""
                SELECT provider_detail FROM mission_control.runtime_execution_attempt
                WHERE {SCOPE} AND binding_key = $4 AND runtime_attempt = $5
                """,
                *args,
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
                INSERT INTO mission_control.runtime_execution_attempt (
                    installation_id, application_id, tenant_id, runtime_execution_attempt_id,
                    binding_key, runtime_attempt, submission_key, disposition,
                    provider_request_digest, agent_server_run_key, provider_detail, started_at,
                    heartbeat_at, lease_expires_at, finished_at, failure_code, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15,
                        $16, $12, $17)
                """,
                *args,
                uuid7(),
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
                mc.WRITER_REF,
            )
        return attempt

    async def update_binding(
        self,
        binding: RuntimeExecutionBinding,
        *,
        expected_version: int,
    ) -> RuntimeExecutionBinding:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, binding.epoch.request_scope)
            prior_payload = await connection.fetchval(
                f"""
                SELECT binding_payload FROM mission_control.runtime_execution_binding
                WHERE {SCOPE} AND binding_key = $4
                FOR UPDATE
                """,
                *args,
                binding.binding_id,
            )
            if prior_payload is None:
                raise LookupError("runtime binding not found")
            prior = RuntimeExecutionBinding.model_validate(_json(prior_payload))
            _validate_binding_update(prior, binding, expected_version)
            result = await connection.execute(
                f"""
                UPDATE mission_control.runtime_execution_binding
                SET status = $5, active = $6, version = $7, binding_payload = $8::jsonb,
                    updated_at = $9
                WHERE {SCOPE} AND binding_key = $4 AND version = $10
                """,
                *args,
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
            args = await mc.begin(connection, epoch.request_scope)
            rows = await connection.fetch(
                f"""
                SELECT provider_detail FROM mission_control.runtime_execution_attempt
                WHERE {SCOPE} AND binding_key = $4
                ORDER BY runtime_attempt
                """,
                *args,
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
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"runtime-command:{scope}:{intervention.command_id}")
            prior = await _intervention_row(connection, args, intervention, lock=True)
            if prior is not None:
                _require_same_intervention(prior, intervention)
                if prior["receipt"] is not None:
                    persisted_receipt = InterventionReceipt.model_validate(_json(prior["receipt"]))
                    if persisted_receipt != receipt:
                        raise RuntimeBindingConflict(
                            "intervention receipt conflicts with prior completion"
                        )
                    return persisted_receipt
                await _record_receipt(connection, args, prior["command_id"], receipt)
                return receipt
            command_id = await _insert_intervention(
                connection, args, intervention, binding_id=receipt.binding_id
            )
            await _record_receipt(connection, args, command_id, receipt)
        return receipt

    async def reserve(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> bool:
        scope = intervention.epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(
                connection, f"runtime-command:{scope}:{intervention.idempotency_key}"
            )
            prior = await _intervention_row(connection, args, intervention)
            if prior is not None:
                if (
                    prior["command_key"] != _command_key(intervention.command_id)
                    or prior["request_digest"] != intervention.request_digest
                    or _json(prior["payload"]) != intervention.model_dump(mode="json")
                ):
                    raise RuntimeBindingConflict(
                        "intervention idempotency identity has conflicting intent"
                    )
                return False
            await _insert_intervention(connection, args, intervention, binding_id=binding_id)
        return True

    async def get_intervention(
        self,
        request_scope: str,
        command_id: str,
    ) -> tuple[RuntimeIntervention, InterventionReceipt] | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                SELECT i.binding_key, i.expected_run_version, i.requested_at, c.payload,
                       (SELECT r.detail FROM mission_control.delivery_report r
                        WHERE r.installation_id = c.installation_id
                          AND r.application_id = c.application_id
                          AND r.tenant_id = c.tenant_id AND r.command_id = c.command_id
                        ORDER BY r.reported_at DESC LIMIT 1) AS receipt
                FROM mission_control.runtime_intervention i
                JOIN mission_control.command c
                  ON c.installation_id = i.installation_id
                 AND c.application_id = i.application_id
                 AND c.tenant_id = i.tenant_id AND c.command_id = i.command_id
                WHERE {scoped("i")} AND i.command_key = $4
                """,
                *args,
                _command_key(command_id),
            )
        if row is None:
            return None
        command = INTERVENTION_ADAPTER.validate_python(_json(row["payload"]))
        if row["receipt"] is None:
            receipt = InterventionReceipt(
                command_id=command.command_id,
                status="reconciliation_required",
                binding_id=row["binding_key"],
                resulting_belllabs_version=row["expected_run_version"],
                reason_code="provider_application_pending",
                recorded_at=row["requested_at"],
            )
        else:
            receipt = InterventionReceipt.model_validate(_json(row["receipt"]))
        return command, receipt


def _command_key(command_id: str) -> str:
    return f"runtime-intervention:{command_id}"


async def _intervention_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    intervention: RuntimeIntervention,
    *,
    lock: bool = False,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT i.command_key, i.command_id, i.request_digest, c.payload,
               (SELECT r.detail FROM mission_control.delivery_report r
                WHERE r.installation_id = c.installation_id
                  AND r.application_id = c.application_id
                  AND r.tenant_id = c.tenant_id AND r.command_id = c.command_id
                ORDER BY r.reported_at DESC LIMIT 1) AS receipt
        FROM mission_control.runtime_intervention i
        JOIN mission_control.command c
          ON c.installation_id = i.installation_id AND c.application_id = i.application_id
         AND c.tenant_id = i.tenant_id AND c.command_id = i.command_id
        WHERE {scoped("i")} AND (i.command_key = $4 OR i.idempotency_key = $5)
        """
        + (" FOR UPDATE OF i" if lock else ""),
        *args,
        _command_key(intervention.command_id),
        intervention.idempotency_key,
    )


def _require_same_intervention(prior: asyncpg.Record, intervention: RuntimeIntervention) -> None:
    if (
        prior["command_key"] != _command_key(intervention.command_id)
        or prior["request_digest"] != intervention.request_digest
        or _json(prior["payload"]) != intervention.model_dump(mode="json")
    ):
        raise RuntimeBindingConflict("intervention command identity has conflicting intent")


async def _insert_intervention(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    intervention: RuntimeIntervention,
    *,
    binding_id: str,
) -> UUID:
    run = await mc.require_run(connection, args, intervention.epoch.belllabs_run_id)
    command_id = uuid7()
    await connection.execute(
        """
        INSERT INTO mission_control.command (
            installation_id, application_id, tenant_id, command_id, command_key, mission_id,
            run_id, activation_id, subordinate_id, target_generation, target_version,
            command_kind, payload, payload_digest, payload_ref, deadline_at, lifecycle, outcome,
            requested_by_actor_ref, version, updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, $9, $10, $11::jsonb, $12, NULL,
                NULL, 'accepted', NULL, $13, 1, $14, $14, $13)
        """,
        *args,
        command_id,
        _command_key(intervention.command_id),
        run["mission_id"],
        run["run_id"],
        intervention.epoch.execution_epoch,
        intervention.expected_belllabs_version,
        f"runtime_intervention:{intervention.kind}",
        _dump(intervention),
        intervention.request_digest,
        mc.WRITER_REF,
        intervention.requested_at,
    )
    await connection.execute(
        """
        INSERT INTO mission_control.runtime_intervention (
            installation_id, application_id, tenant_id, runtime_intervention_id, command_key,
            command_id, binding_key, idempotency_key, request_digest, intervention_kind,
            expected_run_version, expected_checkpoint_key, status, requested_at, recorded_at,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'pending', $13, NULL, $13, $14)
        """,
        *args,
        uuid7(),
        _command_key(intervention.command_id),
        command_id,
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
        intervention.requested_at,
        mc.WRITER_REF,
    )
    return command_id


async def _record_receipt(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    command_id: UUID,
    receipt: InterventionReceipt,
) -> None:
    await connection.execute(
        """
        INSERT INTO mission_control.delivery_report (
            installation_id, application_id, tenant_id, delivery_report_id, command_id,
            report_key, delivery_semantics, reported_at, native_refs, observed_outcome, detail,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '{}', $9, $10::jsonb, $8, $11)
        """,
        *args,
        uuid7(),
        command_id,
        f"{_command_key(receipt.command_id)}:receipt",
        RECEIPT_SEMANTICS,
        receipt.recorded_at,
        _REPORT_OUTCOME[receipt.status],
        _dump(receipt),
        mc.WRITER_REF,
    )
    await connection.execute(
        f"""
        UPDATE mission_control.runtime_intervention
        SET status = $5, recorded_at = $6
        WHERE {SCOPE} AND command_id = $4
        """,
        *args,
        command_id,
        receipt.status,
        receipt.recorded_at,
    )
    await connection.execute(
        f"""
        UPDATE mission_control.command
        SET lifecycle = $5, outcome = $6, version = version + 1, updated_at = $7
        WHERE {SCOPE} AND command_id = $4
        """,
        *args,
        command_id,
        _COMMAND_LIFECYCLE[receipt.status],
        receipt.reason_code,
        receipt.recorded_at,
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


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
