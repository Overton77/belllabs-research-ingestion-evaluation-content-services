"""Scoped async child details, atomically persisted before external submission.

The immutable child contract and the mutable execution and parent-link envelopes are
support records on mission_control (``subordinate_contract``,
``subordinate_execution_detail``, ``subordinate_link_detail``) bound to the parent run by
scoped key, with forced row-level security.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.subordinates.service import AsyncSubagentError
from mission_control.contracts.identities import uuid7
from mission_control.domain.execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    ParentAsyncSubagentLink,
)

_EXECUTION_IDENTITY = (
    "child_execution_id",
    "contract_id",
    "contract_digest",
    "parent_run_id",
    "parent_operation_id",
    "parent_binding_id",
    "objective_ref",
    "context_slice_ref",
    "reservation_id",
)
_LINK_IDENTITY = (
    "link_id",
    "child_execution_id",
    "parent_run_id",
    "parent_operation_id",
    "dependency_class",
    "cancellation_propagation",
    "late_result_policy",
    "fallback_policy",
    "result_admission_policy_ref",
)
_TERMINAL = {
    AsyncSubagentLifecycle.COMPLETED,
    AsyncSubagentLifecycle.FAILED,
    AsyncSubagentLifecycle.CANCELLED,
    AsyncSubagentLifecycle.ORPHANED,
}


def _payload(value: Any) -> dict[str, Any]:
    return json.loads(value) if isinstance(value, str) else dict(value)


def _same_identity(prior: Any, incoming: Any, fields: tuple[str, ...]) -> None:
    if any(getattr(prior, key) != getattr(incoming, key) for key in fields):
        raise AsyncSubagentError("async detail identity collision")


class PostgresAsyncSubagentDetailRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_before_submit(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        link: ParentAsyncSubagentLink,
    ) -> AsyncSubagentExecution:
        contract = AsyncSubagentContract.model_validate(contract.model_dump())
        execution = AsyncSubagentExecution.model_validate(execution.model_dump())
        link = ParentAsyncSubagentLink.model_validate(link.model_dump())
        if (
            execution.contract_id != contract.contract_id
            or execution.contract_digest != contract.contract_digest
        ):
            raise AsyncSubagentError("async execution does not match contract")
        _same_identity(
            execution, link, ("child_execution_id", "parent_run_id", "parent_operation_id")
        )
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                args = await mc.begin(connection, request_scope)
                await connection.execute(
                    "INSERT INTO mission_control.subordinate_contract "
                    "(installation_id, application_id, tenant_id, subordinate_contract_id, "
                    "contract_key, contract_digest, payload, created_at, created_by_actor_ref) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9) "
                    "ON CONFLICT (installation_id, application_id, tenant_id, contract_key) "
                    "DO NOTHING",
                    *args,
                    uuid7(),
                    contract.contract_id,
                    contract.contract_digest,
                    contract.model_dump_json(),
                    execution.created_at,
                    mc.WRITER_REF,
                )
                prior_contract = await connection.fetchval(
                    "SELECT payload FROM mission_control.subordinate_contract "
                    f"WHERE {SCOPE} AND contract_key = $4",
                    *args,
                    contract.contract_id,
                )
                if AsyncSubagentContract.model_validate(_payload(prior_contract)) != contract:
                    raise AsyncSubagentError("async contract identity collision")
                await connection.execute(
                    "INSERT INTO mission_control.subordinate_execution_detail "
                    "(installation_id, application_id, tenant_id, subordinate_execution_detail_id,"
                    " subordinate_key, contract_key, contract_digest, parent_run_key,"
                    " parent_operation_key, execution_generation, payload, updated_at, created_at,"
                    " created_by_actor_ref) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $12, $13) "
                    "ON CONFLICT (installation_id, application_id, tenant_id, subordinate_key) "
                    "DO NOTHING",
                    *args,
                    uuid7(),
                    execution.child_execution_id,
                    execution.contract_id,
                    execution.contract_digest,
                    execution.parent_run_id,
                    execution.parent_operation_id,
                    execution.execution_generation,
                    execution.model_dump_json(),
                    execution.updated_at,
                    mc.WRITER_REF,
                )
                prior = await self._execution_on(connection, args, execution.child_execution_id)
                _same_identity(prior, execution, _EXECUTION_IDENTITY)
                if prior.execution_generation != execution.execution_generation:
                    raise AsyncSubagentError("async child generation collision")
                await connection.execute(
                    "INSERT INTO mission_control.subordinate_link_detail "
                    "(installation_id, application_id, tenant_id, subordinate_link_detail_id,"
                    " subordinate_key, link_key, parent_run_key, parent_operation_key, payload,"
                    " updated_at, created_at, created_by_actor_ref) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $10, $11) "
                    "ON CONFLICT (installation_id, application_id, tenant_id, subordinate_key) "
                    "DO NOTHING",
                    *args,
                    uuid7(),
                    link.child_execution_id,
                    link.link_id,
                    link.parent_run_id,
                    link.parent_operation_id,
                    link.model_dump_json(),
                    link.updated_at,
                    mc.WRITER_REF,
                )
                stored_link = await self._link_on(connection, args, link.child_execution_id)
                _same_identity(stored_link, link, _LINK_IDENTITY)
                return prior
        except asyncpg.UniqueViolationError:
            raise AsyncSubagentError("async detail identity collision") from None

    @staticmethod
    async def _execution_on(
        connection: asyncpg.Connection, args: tuple[Any, ...], child: str, *, lock: bool = False
    ) -> AsyncSubagentExecution:
        row = await connection.fetchval(
            "SELECT payload FROM mission_control.subordinate_execution_detail "
            f"WHERE {SCOPE} AND subordinate_key = $4" + (" FOR UPDATE" if lock else ""),
            *args,
            child,
        )
        if row is None:
            raise AsyncSubagentError("async child execution not found")
        return AsyncSubagentExecution.model_validate(_payload(row))

    @staticmethod
    async def _link_on(
        connection: asyncpg.Connection, args: tuple[Any, ...], child: str, *, lock: bool = False
    ) -> ParentAsyncSubagentLink:
        row = await connection.fetchval(
            "SELECT payload FROM mission_control.subordinate_link_detail "
            f"WHERE {SCOPE} AND subordinate_key = $4" + (" FOR UPDATE" if lock else ""),
            *args,
            child,
        )
        if row is None:
            raise AsyncSubagentError("async child link not found")
        return ParentAsyncSubagentLink.model_validate(_payload(row))

    async def get_execution(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentExecution:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await self._execution_on(connection, args, child_execution_id)

    async def get_contract(self, request_scope: str, contract_id: str) -> AsyncSubagentContract:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchval(
                "SELECT payload FROM mission_control.subordinate_contract "
                f"WHERE {SCOPE} AND contract_key = $4",
                *args,
                contract_id,
            )
            if row is None:
                raise AsyncSubagentError("async subagent contract not found")
            return AsyncSubagentContract.model_validate(_payload(row))

    async def get_link(
        self, request_scope: str, child_execution_id: str
    ) -> ParentAsyncSubagentLink:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await self._link_on(connection, args, child_execution_id)

    async def save_execution(self, request_scope: str, execution: AsyncSubagentExecution) -> None:
        execution = AsyncSubagentExecution.model_validate(execution.model_dump())
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            prior = await self._execution_on(
                connection, args, execution.child_execution_id, lock=True
            )
            _same_identity(prior, execution, (*_EXECUTION_IDENTITY, "created_at"))
            if (
                execution.updated_at < prior.updated_at
                or execution.execution_generation < prior.execution_generation
                or (prior.lifecycle in _TERMINAL and execution.lifecycle != prior.lifecycle)
                or (
                    prior.lifecycle != AsyncSubagentLifecycle.PROPOSED
                    and execution.lifecycle == AsyncSubagentLifecycle.PROPOSED
                )
                or (
                    prior.lifecycle
                    not in {AsyncSubagentLifecycle.PROPOSED, AsyncSubagentLifecycle.ADMITTED}
                    and execution.lifecycle == AsyncSubagentLifecycle.ADMITTED
                )
                or (
                    prior.lifecycle
                    in {AsyncSubagentLifecycle.RUNNING, AsyncSubagentLifecycle.WAITING}
                    and execution.lifecycle == AsyncSubagentLifecycle.SUBMITTED
                )
                or (
                    prior.result_manifest is not None
                    and prior.result_manifest != execution.result_manifest
                )
                or (prior.provider_run_id is not None and execution.provider_run_id is None)
                or (prior.result_output_text is not None and execution.result_output_text is None)
            ):
                raise AsyncSubagentError("stale async execution update")
            await connection.execute(
                "UPDATE mission_control.subordinate_execution_detail "
                "SET payload = $5::jsonb, updated_at = $6, execution_generation = $7 "
                f"WHERE {SCOPE} AND subordinate_key = $4",
                *args,
                execution.child_execution_id,
                execution.model_dump_json(),
                execution.updated_at,
                execution.execution_generation,
            )

    async def save_link(self, request_scope: str, link: ParentAsyncSubagentLink) -> None:
        link = ParentAsyncSubagentLink.model_validate(link.model_dump())
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            prior = await self._link_on(connection, args, link.child_execution_id, lock=True)
            _same_identity(prior, link, (*_LINK_IDENTITY, "created_at", "timeout_at"))
            if (
                link.updated_at < prior.updated_at
                or link.settlement_revision < prior.settlement_revision
                or (prior.cancellation_requested and not link.cancellation_requested)
                or (prior.settled and not link.settled)
            ):
                raise AsyncSubagentError("stale async link update")
            # Timestamp ordering alone is insufficient: a cancellation may have read
            # the link before a concurrent result admission and carry a later time.
            # Reject lost facts instead of silently erasing another command's result.
            for field in (
                "admitted_manifest_digest",
                "reconciliation_decision",
                "adopted_provider_run_id",
            ):
                old = getattr(prior, field)
                if old is not None and getattr(link, field) != old:
                    raise AsyncSubagentError("stale async link protected fact update")
            if prior.result_decision is not None:
                allowed = {prior.result_decision}
                if prior.result_decision == "defer":
                    allowed.update({"admit", "conditionally_admit", "reject"})
                elif prior.result_decision == "conditionally_admit":
                    allowed.add("admit")
                if link.result_decision not in allowed:
                    raise AsyncSubagentError("stale async result decision update")
            if (
                (prior.cancellation_reason is not None and link.cancellation_reason is None)
                or (prior.cancellation_receipt is not None and link.cancellation_receipt is None)
                or (
                    prior.cancellation_receipt == "provider_acknowledged"
                    and link.cancellation_receipt != "provider_acknowledged"
                )
                or (prior.usage_disposition is not None and link.usage_disposition is None)
                or (prior.usage_disposition == "settled" and link.usage_disposition != "settled")
            ):
                raise AsyncSubagentError("stale async link receipt update")
            messages = {message.message_id: message for message in link.messages}
            receipt_order = {
                "accepted": 0,
                "claimed": 1,
                "provider_applied": 2,
                "checkpoint_committed": 3,
                "terminal_rejected": 3,
            }
            for old in prior.messages:
                new = messages.get(old.message_id)
                if (
                    new is None
                    or old.model_dump(exclude={"receipt"}) != new.model_dump(exclude={"receipt"})
                    or receipt_order[new.receipt] < receipt_order[old.receipt]
                    or (receipt_order[old.receipt] == 3 and old.receipt != new.receipt)
                ):
                    raise AsyncSubagentError("stale async message update")
            await connection.execute(
                "UPDATE mission_control.subordinate_link_detail "
                "SET payload = $5::jsonb, updated_at = $6 "
                f"WHERE {SCOPE} AND subordinate_key = $4",
                *args,
                link.child_execution_id,
                link.model_dump_json(),
                link.updated_at,
            )
