"""Actual PostgreSQL transactional detail replacement; no provider or Mongo calls."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.async_subagents.async_subagent_detail_repository import (
    PostgresAsyncSubagentDetailRepository,
)
from mission_control.adapters.postgres.connections import apply_application_migrations
from mission_control.application.subordinates.service import AsyncSubagentError
from mission_control.domain.execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
    ParentAsyncSubagentLink,
)
from tests.acceptance.control_plane.test_wp_cp_045 import contract

NOW = datetime(2026, 10, 3, tzinfo=UTC)


@pytest_asyncio.fixture
async def detail_pool():
    dsn = os.environ.get("TEST_APPLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.fail("Set TEST_APPLICATION_POSTGRES_DSN to an isolated disposable database")
    owner = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        await apply_application_migrations(owner)
    finally:
        await owner.close()

    async def runtime_role(connection):
        await connection.execute("SET ROLE belllabs_control_runtime")

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, setup=runtime_role)
    try:
        yield pool
    finally:
        await pool.close()


def details():
    specification = contract()
    execution = AsyncSubagentExecution(
        child_execution_id="child-1",
        contract_id=specification.contract_id,
        contract_digest=specification.contract_digest,
        parent_run_id="parent-1",
        parent_operation_id="operation-1",
        parent_binding_id="binding-1",
        execution_generation=1,
        objective_ref="objective-1",
        context_slice_ref="slice-1",
        reservation_id="reservation-1",
        lifecycle=AsyncSubagentLifecycle.PROPOSED,
        created_at=NOW,
        updated_at=NOW,
    )
    link = ParentAsyncSubagentLink(
        link_id="link-1",
        child_execution_id=execution.child_execution_id,
        parent_run_id=execution.parent_run_id,
        parent_operation_id=execution.parent_operation_id,
        dependency_class=AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
        timeout_at=NOW + timedelta(seconds=300),
        cancellation_propagation="required",
        late_result_policy="quarantine",
        fallback_policy="degrade",
        result_admission_policy_ref="policy:async-result:v1",
        created_at=NOW,
        updated_at=NOW,
    )
    return specification, execution, link


async def test_atomic_create_replay_and_scope(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    results = await asyncio.gather(
        *[repository.create_before_submit(scope, specification, execution, link) for _ in range(8)]
    )
    assert results == [execution] * 8
    assert await repository.get_contract(scope, specification.contract_id) == specification
    assert await repository.get_link(scope, execution.child_execution_id) == link
    with pytest.raises(AsyncSubagentError, match="not found"):
        await repository.get_execution("other-" + scope, execution.child_execution_id)
    with pytest.raises(AsyncSubagentError, match="identity collision"):
        await repository.create_before_submit(
            scope, specification, execution.model_copy(update={"objective_ref": "changed"}), link
        )
    # A link-id collision rolls back both preceding contract and execution inserts.
    new_contract = AsyncSubagentContract.create(
        **{**specification.model_dump(exclude={"contract_digest"}), "contract_id": "new-contract"}
    )
    new_execution = execution.model_copy(
        update={
            "child_execution_id": "new-child",
            "contract_id": new_contract.contract_id,
            "contract_digest": new_contract.contract_digest,
        }
    )
    with pytest.raises(AsyncSubagentError, match="identity collision"):
        await repository.create_before_submit(
            scope,
            new_contract,
            new_execution,
            link.model_copy(update={"child_execution_id": new_execution.child_execution_id}),
        )
    with pytest.raises(AsyncSubagentError, match="not found"):
        await repository.get_contract(scope, new_contract.contract_id)
    with pytest.raises(AsyncSubagentError, match="not found"):
        await repository.get_execution(scope, new_execution.child_execution_id)


async def test_execution_monotonic_save_and_identity(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    admitted = execution.model_copy(update={"lifecycle": AsyncSubagentLifecycle.ADMITTED})
    await repository.save_execution(scope, admitted)  # same-clock sequential transition is legal
    with pytest.raises(AsyncSubagentError, match="stale"):
        await repository.save_execution(scope, execution)
    failed = admitted.model_copy(
        update={
            "lifecycle": AsyncSubagentLifecycle.FAILED,
            "updated_at": NOW + timedelta(seconds=1),
        }
    )
    await repository.save_execution(scope, failed)
    with pytest.raises(AsyncSubagentError, match="stale"):
        await repository.save_execution(
            scope,
            admitted.model_copy(
                update={
                    "updated_at": NOW + timedelta(seconds=2),
                }
            ),
        )
    with pytest.raises(AsyncSubagentError, match="identity collision"):
        await repository.save_execution(scope, failed.model_copy(update={"parent_run_id": "other"}))
    assert await repository.get_execution(scope, execution.child_execution_id) == failed
    assert await repository.create_before_submit(scope, specification, execution, link) == failed


async def test_later_unbound_execution_cannot_erase_provider_identity(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    running = execution.model_copy(
        update={
            "lifecycle": AsyncSubagentLifecycle.RUNNING,
            "provider_thread_id": "thread-1",
            "provider_run_id": "provider-1",
        }
    )
    await repository.save_execution(scope, running)
    with pytest.raises(AsyncSubagentError, match="stale"):
        await repository.save_execution(
            scope,
            running.model_copy(
                update={
                    "provider_thread_id": None,
                    "provider_run_id": None,
                    "updated_at": NOW + timedelta(seconds=1),
                }
            ),
        )
    assert await repository.get_execution(scope, execution.child_execution_id) == running


async def test_link_cancellation_and_message_receipts_do_not_regress(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    message = AsyncSubagentMessage(
        message_id="message-1",
        child_execution_id=execution.child_execution_id,
        direction="parent_to_child",
        target_sequence=1,
        correlation_id="correlation-1",
        payload_ref="payload-1",
        context_authority="instruction",
        created_at=NOW,
    )
    accepted = link.model_copy(update={"messages": (message,), "cancellation_requested": True})
    await repository.save_link(scope, accepted)
    with pytest.raises(AsyncSubagentError, match="stale"):
        await repository.save_link(scope, link)
    applied = accepted.model_copy(
        update={
            "messages": (message.model_copy(update={"receipt": "provider_applied"}),),
        }
    )
    await repository.save_link(scope, applied)
    with pytest.raises(AsyncSubagentError, match="stale async message"):
        await repository.save_link(scope, accepted)
    assert await repository.get_link(scope, execution.child_execution_id) == applied


async def test_missing_context_and_immutable_contract_grants(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    async with detail_pool.acquire() as connection:
        assert await connection.fetchval("SELECT current_user") == "belllabs_control_runtime"
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM belllabs_control.async_subagent_execution_details "
            "WHERE request_scope=$1)",
            scope,
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE belllabs_control.async_subagent_contract_details SET payload=payload"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM belllabs_control.async_subagent_link_details")


async def test_stale_cancellation_cannot_erase_admission_with_later_timestamp(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    admitted = link.model_copy(
        update={
            "result_decision": "admit",
            "admitted_manifest_digest": "sha256:" + "a" * 64,
            "updated_at": NOW + timedelta(seconds=1),
        }
    )
    await repository.save_link(scope, admitted)
    stale_cancellation = link.model_copy(
        update={
            "cancellation_requested": True,
            "cancellation_reason": "operator request",
            "updated_at": NOW + timedelta(seconds=2),
        }
    )
    with pytest.raises(AsyncSubagentError, match="stale"):
        await repository.save_link(scope, stale_cancellation)
    assert await repository.get_link(scope, execution.child_execution_id) == admitted
    # Reloading and preserving committed facts permits the requested cancellation.
    cancelled = admitted.model_copy(
        update={
            "cancellation_requested": True,
            "cancellation_reason": "operator request",
            "cancellation_receipt": "provider_acknowledged",
            "reconciliation_decision": "adopt_provider_run",
            "adopted_provider_run_id": "provider-1",
            "updated_at": NOW + timedelta(seconds=2),
        }
    )
    await repository.save_link(scope, cancelled)
    for field in ("cancellation_receipt", "reconciliation_decision", "adopted_provider_run_id"):
        with pytest.raises(AsyncSubagentError, match="stale"):
            await repository.save_link(
                scope,
                cancelled.model_copy(
                    update={
                        field: None,
                        "updated_at": NOW + timedelta(seconds=3),
                    }
                ),
            )
    assert await repository.get_link(scope, execution.child_execution_id) == cancelled


async def test_terminal_message_receipt_is_immutable(detail_pool):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    message = AsyncSubagentMessage(
        message_id="terminal-message",
        child_execution_id=execution.child_execution_id,
        direction="parent_to_child",
        target_sequence=1,
        correlation_id="correlation-1",
        payload_ref="payload-1",
        context_authority="instruction",
        created_at=NOW,
        receipt="checkpoint_committed",
    )
    committed = link.model_copy(update={"messages": (message,)})
    await repository.save_link(scope, committed)
    with pytest.raises(AsyncSubagentError, match="stale async message"):
        await repository.save_link(
            scope,
            committed.model_copy(
                update={
                    "messages": (message.model_copy(update={"receipt": "terminal_rejected"}),),
                    "updated_at": NOW + timedelta(seconds=1),
                }
            ),
        )
    assert await repository.get_link(scope, execution.child_execution_id) == committed


@pytest.mark.parametrize(
    "table,identity",
    [
        ("async_subagent_execution_details", "child_execution_id"),
        ("async_subagent_link_details", "link_id"),
    ],
)
@pytest.mark.parametrize("mutation", ["mismatch", "missing", "null"])
async def test_database_rejects_payload_identity_drift(detail_pool, table, identity, mutation):
    repository = PostgresAsyncSubagentDetailRepository(detail_pool)
    scope = str(uuid4())
    specification, execution, link = details()
    await repository.create_before_submit(scope, specification, execution, link)
    async with detail_pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('belllabs.request_scope', $1, true)", scope)
        # Identifiers are closed pytest parameters, never supplied by request data.
        expression = {
            "mismatch": f"jsonb_set(payload, '{{{identity}}}', '\"another-identity\"')",
            "missing": f"payload - '{identity}'",
            "null": f"jsonb_set(payload, '{{{identity}}}', 'null'::jsonb)",
        }[mutation]
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE belllabs_control.{table} SET payload={expression} "
                "WHERE request_scope=$1 AND child_execution_id=$2",
                scope,
                execution.child_execution_id,
            )
    assert await repository.get_execution(scope, execution.child_execution_id) == execution
    assert await repository.get_link(scope, execution.child_execution_id) == link
