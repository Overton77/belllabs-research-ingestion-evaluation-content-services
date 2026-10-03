"""Real PostgreSQL tests; run with TEST_APPLICATION_POSTGRES_DSN explicitly set."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.connections import apply_application_migrations
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
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.domain.execution.contracts import OperationSettlement
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.fixtures.goal_directed_journaled import goal_revision
from tests.unit.operations.test_operation_execution import operation_request

NOW = datetime(2026, 10, 3, tzinfo=UTC)


@pytest_asyncio.fixture
async def document_pool():
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


@pytest.mark.asyncio
async def test_concurrent_replay_keeps_first_observation_and_rejects_changes(document_pool):
    store = PostgresDocumentStore(document_pool)
    scope = str(uuid4())
    arguments = dict(
        request_scope=scope,
        contract="goal.revision/1",
        identity="revision-1",
        payload={"tuple": ("one", "two")},
        recorded_at=NOW,
    )
    initial = await store.put(**arguments)
    replays = await asyncio.gather(
        *[store.put(**{**arguments, "recorded_at": NOW + timedelta(minutes=n)}) for n in range(8)]
    )
    assert all(row.recorded_at == initial.recorded_at for row in replays)
    assert all(row.payload == {"tuple": ["one", "two"]} for row in replays)
    with pytest.raises(IdempotencyConflict):
        await store.put(**{**arguments, "payload": {"tuple": ["changed"]}})


@pytest.mark.asyncio
async def test_sandbox_snapshot_claims_metadata_and_clones_are_scoped(document_pool):
    from mission_control.adapters.postgres.workspaces.snapshot_repository import (
        PostgresSandboxSnapshotRepository,
    )
    from mission_control.domain.execution.contracts import (
        ReacquiredRuntimeResources,
        SnapshotCloneRecord,
    )
    from tests.unit.workspaces.test_sandbox_snapshots import create_request, service

    source, *_ = service()
    snapshot = await source.create(create_request())
    scope = str(uuid4())
    snapshot = snapshot.model_copy(update={"request_scope": scope})
    repository = PostgresSandboxSnapshotRepository(document_pool, request_scope=scope)
    other = PostgresSandboxSnapshotRepository(document_pool, request_scope=scope + "-other")
    claims = await asyncio.gather(
        *[
            repository.claim_creation(snapshot.snapshot_id, snapshot.creation_identity, NOW)
            for _ in range(8)
        ]
    )
    assert claims.count(True) == 1
    with pytest.raises(IdempotencyConflict):
        await repository.claim_creation(snapshot.snapshot_id, "different-intent", NOW)
    assert await repository.create_snapshot(snapshot) == snapshot
    restarted = PostgresSandboxSnapshotRepository(document_pool, request_scope=scope)
    assert await restarted.get_snapshot(snapshot.snapshot_id) == snapshot
    assert await restarted.create_snapshot(snapshot) == snapshot
    assert await other.get_snapshot(snapshot.snapshot_id) is None
    with pytest.raises(ValueError, match="request scope"):
        await other.create_snapshot(snapshot)
    with pytest.raises(IdempotencyConflict):
        await repository.create_snapshot(snapshot.model_copy(update={"provider_snapshot_id": "x"}))
    with pytest.raises(IdempotencyConflict, match="creation identity"):
        await repository.create_snapshot(snapshot.model_copy(update={"snapshot_id": "another-id"}))
    assert await repository.get_snapshot("another-id") is None
    clone = SnapshotCloneRecord(
        clone_id="clone-one",
        snapshot_id=snapshot.snapshot_id,
        parent_workspace_id=snapshot.source_workspace_id,
        target_namespace_id="clone-namespace",
        target_workspace_id="clone-workspace",
        binding_id="clone-binding",
        resources=ReacquiredRuntimeResources(),
        created_at=NOW,
    )
    assert await repository.claim_clone("clone-intent", clone.clone_id, NOW)
    assert not await restarted.claim_clone("clone-intent", clone.clone_id, NOW)
    assert await repository.create_clone(clone) == clone
    assert await restarted.get_clone(clone.clone_id) == clone
    with pytest.raises(IdempotencyConflict, match="clone target"):
        await repository.create_clone(clone.model_copy(update={"clone_id": "another-clone"}))
    assert await repository.get_clone("another-clone") is None
    assert await other.get_clone(clone.clone_id) is None
    with pytest.raises(IdempotencyConflict, match="current request scope"):
        await other.create_clone(clone)


@pytest.mark.asyncio
async def test_rls_and_no_mutation_authority(document_pool):
    store = PostgresDocumentStore(document_pool)
    scope = str(uuid4())
    await store.put(
        request_scope=scope,
        contract="goal.revision/1",
        identity="x",
        payload={"value": 1},
        recorded_at=NOW,
    )
    assert (
        await store.get(request_scope="other-" + scope, contract="goal.revision/1", identity="x")
        is None
    )
    async with document_pool.acquire() as connection, connection.transaction():
        assert await connection.fetchval("SELECT current_user") == "belllabs_control_runtime"
        await store.set_scope(connection, "other-" + scope)
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM belllabs_control.immutable_documents "
            "WHERE request_scope=$1)",
            scope,
        )
    async with document_pool.acquire() as connection:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                "UPDATE belllabs_control.immutable_documents SET identity=identity"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM belllabs_control.immutable_documents")
        async with connection.transaction():
            await store.set_scope(connection, "other-" + scope)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await store.put_on(
                    connection,
                    request_scope=scope,
                    contract="goal.revision/1",
                    identity="cross",
                    payload={"value": 1},
                    recorded_at=NOW,
                )


@pytest.mark.asyncio
async def test_goal_revision_and_templates_replay_and_scope(document_pool):
    scope = str(uuid4())
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
            request_scope="other-" + scope,
            semantic_input_binding_ref="input-1",
            operation_role="executor",
            run_id="run-" + scope,
        )


@pytest.mark.asyncio
async def test_stagegraph_fork_templates_and_batch_rollback(document_pool):
    scope = str(uuid4())
    repository = PostgresStageGraphOperationTemplateRepository(document_pool)
    request = operation_request().model_copy(update={"request_scope": scope})
    arguments = dict(request_scope=scope, semantic_input_binding_ref="source", recorded_at=NOW)
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
    scope = str(uuid4())
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
    assert (
        await repository.get_binding_by_id(binding.binding_id, request_scope="other-" + scope)
        is None
    )
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
        await repository.settle(settlement, request_scope="other-" + scope)
