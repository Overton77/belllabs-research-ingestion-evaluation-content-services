"""Immutable goal/StageGraph/operation detail documents on mission_control.runtime_document.

Each test gets a fresh disposable common database and a restricted login of
`mission_control_runtime` (real grants, forced RLS, append-only trigger).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.documents import PostgresDocumentStore
from mission_control.adapters.postgres.operations.operation_binding_repository import (
    PostgresOperationBindingRepository,
)
from mission_control.adapters.postgres.orchestration.goal_directed_repository import (
    PostgresGoalDirectedDocumentRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.postgres.scope import apply_scope, scope_values
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.execution.contracts import OperationSettlement
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.fixtures.goal_directed_journaled import goal_revision
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    create_common_database,
    drop_common_database,
)
from tests.unit.operations.test_operation_execution import operation_request

pytestmark = pytest.mark.common_db
_DATABASES: dict[int, CommonDatabase] = {}

NOW = datetime(2026, 10, 3, tzinfo=UTC)


@pytest_asyncio.fixture
async def document_pool() -> AsyncIterator[asyncpg.Pool]:
    """A restricted runtime pool on a fresh common database (scopes via `scopes_for`)."""

    database = await create_common_database()
    pool = await database.pool(max_size=8)
    _DATABASES[id(pool)] = database
    try:
        yield pool
    finally:
        _DATABASES.pop(id(pool), None)
        await pool.close()
        await drop_common_database(database)


def scopes_for(pool: asyncpg.Pool) -> tuple[str, str]:
    database = _DATABASES[id(pool)]
    return database.scope("tenant-1"), database.scope("tenant-2")


@pytest.mark.asyncio
async def test_concurrent_replay_keeps_first_observation_and_rejects_changes(document_pool):
    store = PostgresDocumentStore(document_pool)
    scope, _other_scope = scopes_for(document_pool)
    arguments = {
        "request_scope": scope,
        "contract": "goal.revision/1",
        "identity": "revision-1",
        "payload": {"tuple": ("one", "two")},
        "recorded_at": NOW,
    }
    initial = await store.put(**arguments)
    replays = await asyncio.gather(
        *[store.put(**{**arguments, "recorded_at": NOW + timedelta(minutes=n)}) for n in range(8)]
    )
    assert all(row.recorded_at == initial.recorded_at for row in replays)
    assert all(row.payload == {"tuple": ["one", "two"]} for row in replays)
    with pytest.raises(IdempotencyConflict):
        await store.put(**{**arguments, "payload": {"tuple": ["changed"]}})


@pytest.mark.asyncio
async def test_rls_and_no_mutation_authority(document_pool):
    store = PostgresDocumentStore(document_pool)
    scope, other_scope = scopes_for(document_pool)
    await store.put(
        request_scope=scope,
        contract="goal.revision/1",
        identity="x",
        payload={"value": 1},
        recorded_at=NOW,
    )
    assert (
        await store.get(request_scope=other_scope, contract="goal.revision/1", identity="x") is None
    )
    async with document_pool.acquire() as connection, connection.transaction():
        assert await connection.fetchval(
            "SELECT pg_has_role(current_user, 'mission_control_runtime', 'MEMBER')"
        )
        await store.set_scope(connection, other_scope)
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM mission_control.runtime_document)"
        )
    async with document_pool.acquire() as connection:
        # Missing scope context: no rows at all.
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM mission_control.runtime_document)"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE mission_control.runtime_document SET identity=identity"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM mission_control.runtime_document")
        async with connection.transaction():
            # A write cannot borrow another tenant's rows: the store re-binds its own scope,
            # and a forged row for a different tenant is refused by forced RLS.
            await apply_scope(connection, other_scope)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "INSERT INTO mission_control.runtime_document (installation_id,"
                    " application_id, tenant_id, runtime_document_id, contract, identity,"
                    " payload, digest, recorded_at, created_at, created_by_actor_ref)"
                    " VALUES ($1, $2, $3, gen_random_uuid(), 'goal.revision/1', 'forged',"
                    " '{}'::jsonb, $4, now(), now(), 'x')",
                    *scope_values(parse_request_scope(scope)),
                    "sha256:" + "0" * 64,
                )
        async with connection.transaction():
            await store.set_scope(connection, other_scope)
            with pytest.raises(ValueError):
                await store.put_on(
                    connection,
                    request_scope="tenant-1",
                    contract="goal.revision/1",
                    identity="cross",
                    payload={"value": 1},
                    recorded_at=NOW,
                )


@pytest.mark.asyncio
async def test_goal_revision_and_templates_replay_and_scope(document_pool):
    scope, other_scope = scopes_for(document_pool)
    repository = PostgresGoalDirectedDocumentRepository(document_pool)
    revision = goal_revision("run-" + scope)
    reference = await repository.persist_revision(scope, "run-" + scope, revision, NOW)
    assert reference == await repository.persist_revision(
        scope,
        "run-" + scope,
        revision,
        NOW + timedelta(minutes=5),
    )
    with pytest.raises(IdempotencyConflict):
        await repository.persist_revision(
            scope,
            "run-" + scope,
            replace(revision, tactical_changes=("different",)),
            NOW,
        )
    request = operation_request().model_copy(update={"request_scope": scope})
    await repository.persist_templates(
        request_scope=scope,
        semantic_input_binding_ref="input-1",
        executor=request,
        verifier=request,
        recorded_at=NOW,
    )
    assert request == await repository.get_template(
        request_scope=scope,
        semantic_input_binding_ref="input-1",
        operation_role="executor",
        run_id="run-" + scope,
    )
    with pytest.raises(ValueError, match="unavailable"):
        await repository.get_template(
            request_scope=other_scope,
            semantic_input_binding_ref="input-1",
            operation_role="executor",
            run_id="run-" + scope,
        )


@pytest.mark.asyncio
async def test_stagegraph_fork_templates_and_batch_rollback(document_pool):
    scope, _other_scope = scopes_for(document_pool)
    repository = PostgresStageGraphOperationTemplateRepository(document_pool)
    request = operation_request().model_copy(update={"request_scope": scope})
    arguments = {"request_scope": scope, "semantic_input_binding_ref": "source", "recorded_at": NOW}
    await repository.persist_templates(**arguments, templates={"z": request})
    changed = operation_request(prompt="other").model_copy(update={"request_scope": scope})
    with pytest.raises(IdempotencyConflict):
        await repository.persist_templates(**arguments, templates={"a": request, "z": changed})
    assert await repository.list_templates(
        request_scope=scope, semantic_input_binding_ref="source"
    ) == {"z": request}
    await repository.persist_templates(
        **{**arguments, "semantic_input_binding_ref": "fork"},
        templates={"z": changed},
    )
    assert (
        await repository.get_template(
            request_scope=scope,
            semantic_input_binding_ref="fork",
            operation_request_key="z",
            run_id="fork",
        )
        == changed
    )


@pytest.mark.asyncio
async def test_operation_binding_claim_settlement_concurrency(document_pool):
    scope, other_scope = scopes_for(document_pool)
    repository = PostgresOperationBindingRepository(document_pool)
    request = operation_request().model_copy(update={"request_scope": scope})
    binding = bind_operation_execution_request(request)
    results = await asyncio.gather(
        *[repository.create_binding(binding, request_scope=scope) for _ in range(6)]
    )
    assert all(result == binding for result in results)
    claims = await asyncio.gather(*[repository.claim_execution(binding) for _ in range(6)])
    assert sum(claims) == 1
    assert await repository.get_binding_by_id(binding.binding_id, request_scope=scope) == binding
    assert await repository.get_binding_by_id(binding.binding_id, request_scope=other_scope) is None
    with pytest.raises(IdempotencyConflict):
        await repository.create_binding(
            binding.model_copy(update={"request_fingerprint": "sha256:" + "0" * 64}),
            request_scope=scope,
        )
    settlement = OperationSettlement(
        settlement_id="settlement-1",
        binding_id=binding.binding_id,
        status="completed",
        output_text="preserved",
        settled_at=NOW,
    )
    assert await repository.settle(settlement, request_scope=scope) == settlement
    assert (
        await repository.settle(
            settlement.model_copy(update={"settled_at": NOW + timedelta(hours=1)}),
            request_scope=scope,
        )
        == settlement
    )
    with pytest.raises(IdempotencyConflict):
        await repository.settle(
            settlement.model_copy(update={"output_text": "changed"}), request_scope=scope
        )
    with pytest.raises(ValueError, match="outside request scope"):
        await repository.settle(settlement, request_scope=other_scope)
