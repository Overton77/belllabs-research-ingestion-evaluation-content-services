"""RRM-006 on PostgreSQL: migration 0024, least privilege, RLS, and fork crash recovery.

Every fork write runs under `SET ROLE belllabs_control_runtime` (no superuser, no
`BYPASSRLS`); reads are re-checked under `belllabs_operations_readonly`. The saga is driven
through run-control admission with injected crashes after the admission commit and after the
materialization commit: each retry reconciles from PostgreSQL and the fork ends with exactly
one durable receipt, one admitted derived run, and one set of reuse decisions.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` (disposable stack).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.runtime.postgres_run_forks import (
    PostgresForkMaterializationStore,
    PostgresForkSourceReader,
    PostgresRunSnapshotRepository,
)
from app.application.runtime.postgres_stage3_kernel_repository import (
    PostgresForkRepository,
    PostgresStage3RetentionRepository,
)
from app.application.runtime.run_forks import (
    ForkPatchPolicyRegistry,
    RecordingForkMaterializer,
    RunControlForkAuthority,
    SemanticForkService,
)
from app.application.runtime.runtime_recovery import RuntimeForkService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.run_control.contracts import RunPhase, StartAction
from app.domain.run_control.errors import IdempotencyConflict
from app.domain.run_control.forks import (
    ForkLineageManifest,
    ForkRejected,
    ReuseCandidate,
    derived_unit_identity,
)
from tests.fixtures.checkpoint_recovery import stage_recovery_unit
from tests.fixtures.run_forks import (
    fork_command,
    review_objective_patch,
    stage_policy,
    technical_snapshot,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.unit.run_control.test_run_control import NOW, WORKFLOW_DIGEST, command
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

RUNTIME_ROLE = "belllabs_control_runtime"
READONLY_ROLE = "belllabs_operations_readonly"
NEW_TABLES = ("run_snapshot_manifests", "run_fork_reuse_decisions")


def _assume(role: str) -> Callable[[asyncpg.Connection], Awaitable[None]]:
    async def setup(connection: asyncpg.Connection) -> None:
        await connection.execute(f"SET ROLE {role}")

    return setup


class AllowRetention:
    async def authorize_deletion(self, **_kwargs: Any) -> bool:
        return True


class CrashAfterAdmission:
    """The admission commits, then the worker loses the result."""

    def __init__(self, inner: RunControlForkAuthority) -> None:
        self._inner = inner
        self.crash = True
        self.admissions = 0

    async def admit_fork(self, request: Any) -> Any:
        admission = await self._inner.admit_fork(request)
        self.admissions += 1
        if self.crash:
            self.crash = False
            raise TimeoutError("admission result lost after commit")
        return admission

    async def reconcile_fork_admission(self, request: Any) -> Any:
        return await self._inner.reconcile_fork_admission(request)


class CrashAfterMaterialization:
    """The materialization commits, then the worker loses the result."""

    def __init__(self, inner: RecordingForkMaterializer) -> None:
        self._inner = inner
        self.crash = True
        self.materializations = 0

    async def materialize(self, request: Any, admission: Any) -> Any:
        materialized = await self._inner.materialize(request, admission)
        self.materializations += 1
        if self.crash:
            self.crash = False
            raise TimeoutError("materialization result lost after commit")
        return materialized

    async def reconcile_materialization(self, request: Any, admission: Any) -> Any:
        return await self._inner.reconcile_materialization(request, admission)


def _candidate(run_id: str, name: str) -> ReuseCandidate:
    unit = stage_recovery_unit(run_id, name)
    return ReuseCandidate(
        unit=unit,
        unit_key=unit.unit_key,
        execution_generation=1,
        binding_id=f"binding:{name}",
        binding_digest=sha256_digest(f"binding:{name}"),
        state_schema_digest=sha256_digest("schema"),
        settlement_id=f"settlement:{name}",
        result_manifest_ref=f"s3://technical/{name}/manifest",
        result_manifest_digest=sha256_digest(f"manifest:{name}"),
        result_manifest_size_bytes=128,
    )


@pytest.mark.asyncio
async def test_migration_0024_forces_rls_and_least_privilege(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            assert await connection.fetchval(
                "SELECT 1 FROM belllabs_control.schema_migrations WHERE version = $1",
                "0024_run_snapshots_semantic_forks_v1.sql",
            )
            for table in NEW_TABLES:
                security = await connection.fetchrow(
                    """
                    SELECT relrowsecurity, relforcerowsecurity FROM pg_class
                    WHERE oid = ('belllabs_control.' || $1)::regclass
                    """,
                    table,
                )
                assert security is not None and tuple(security) == (True, True)
                qualified = f"belllabs_control.{table}"
                privileges = {
                    (role, action): await connection.fetchval(
                        "SELECT has_table_privilege($1, $2, $3)", role, qualified, action
                    )
                    for role in (RUNTIME_ROLE, READONLY_ROLE)
                    for action in ("SELECT", "INSERT", "UPDATE", "DELETE")
                }
                assert privileges == {
                    (RUNTIME_ROLE, "SELECT"): True,
                    (RUNTIME_ROLE, "INSERT"): True,
                    (RUNTIME_ROLE, "UPDATE"): False,
                    (RUNTIME_ROLE, "DELETE"): False,
                    (READONLY_ROLE, "SELECT"): True,
                    (READONLY_ROLE, "INSERT"): False,
                    (READONLY_ROLE, "UPDATE"): False,
                    (READONLY_ROLE, "DELETE"): False,
                }
            relationship = await connection.fetchval(
                """
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conname = 'runtime_lineage_edges_relationship_check'
                """
            )
            for value in ("derived_from", "seeded_from", "reuses", "contains", "claims"):
                assert f"'{value}'" in relationship
            shape = await connection.fetchval(
                """
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conname = 'runtime_fork_requests_v2_shape'
                """
            )
            assert "source_snapshot_id IS NOT NULL" in shape
            assert "source_binding_id IS NULL" in shape
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_fork_saga_recovers_crashes_under_the_runtime_role_with_one_receipt(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime: asyncpg.Pool | None = None
    readonly: asyncpg.Pool | None = None
    try:
        await reset_application_schema(owner)
        runtime = await asyncpg.create_pool(
            dsn=test_application_postgres_dsn, min_size=1, max_size=6, setup=_assume(RUNTIME_ROLE)
        )
        readonly = await asyncpg.create_pool(
            dsn=test_application_postgres_dsn, min_size=1, max_size=2, setup=_assume(READONLY_ROLE)
        )
        async with runtime.acquire() as connection:
            assert await connection.fetchval("SELECT current_user") == RUNTIME_ROLE
            assert not await connection.fetchval(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
        repository = PostgresRunControlRepository(runtime)
        run_control, _ = run_control_service(repository)  # type: ignore[arg-type]
        admitted = await run_control.admit(run_request(request_id="rrm006-source"))
        assert admitted.run_id is not None
        source = admitted.run_id
        await run_control.execute(command(source, 1, "rrm006-source-start", StartAction()))
        snapshot = technical_snapshot(
            source,
            candidates=(_candidate(source, "draft"), _candidate(source, "review")),
        )
        snapshots = PostgresRunSnapshotRepository(runtime)
        assert await snapshots.put(snapshot) == snapshot
        assert await snapshots.put(snapshot) == snapshot  # idempotent
        conflicting = technical_snapshot(source).model_copy(update={"taken_at": NOW})
        with pytest.raises(ForkRejected) as stale:
            await snapshots.put(conflicting)
        assert stale.value.code == "stale_snapshot"
        assert await snapshots.get("tenant-2", snapshot.snapshot_id) is None

        store = PostgresForkMaterializationStore(runtime)
        authority = CrashAfterAdmission(RunControlForkAuthority(run_control, repository))
        materializer = CrashAfterMaterialization(
            RecordingForkMaterializer(store, retention=lambda at: at + timedelta(days=90))
        )
        fork_repository = PostgresForkRepository(runtime)
        policies = ForkPatchPolicyRegistry()
        policies.register(WORKFLOW_DIGEST, stage_policy())
        forks = SemanticForkService(
            snapshots=snapshots,
            saga=RuntimeForkService(
                repository=fork_repository, authority=authority, materializer=materializer
            ),
            policies=policies,
        )
        intent = fork_command(
            snapshot, changes=(review_objective_patch(),), invalidation_frontier=("review",)
        )

        with pytest.raises(TimeoutError, match="admission result lost"):
            await forks.fork(intent)
        with pytest.raises(TimeoutError, match="materialization result lost"):
            await forks.fork(intent)
        receipt = await forks.fork(intent)
        replay = await forks.fork(
            fork_command(
                snapshot,
                changes=(review_objective_patch(),),
                invalidation_frontier=("review",),
                requested_at_offset=timedelta(minutes=30),
            )
        )

        assert replay == receipt
        assert authority.admissions == 1  # reconciled from the recorded admission decision
        assert materializer.materializations == 1  # reconciled from the recorded lineage
        assert receipt.target_execution_epoch == 1
        derived = await run_control.get_run("tenant-1", receipt.target_run_id)
        assert (derived.phase, derived.version) == (RunPhase.PENDING, 1)
        draft_key = derived_unit_identity(
            stage_recovery_unit(source, "draft"), receipt.target_run_id
        ).unit_key
        review_key = derived_unit_identity(
            stage_recovery_unit(source, "review"), receipt.target_run_id
        ).unit_key
        decisions = await store.list_reuse_decisions("tenant-1", receipt.request_id)
        assert {(item.derived_unit_key, item.decision) for item in decisions} == {
            (draft_key, "reuse"),
            (review_key, "invalidated"),
        }
        assert receipt.lineage.reused_unit_keys == (draft_key,)
        assert (await store.fork_of_run("tenant-1", receipt.target_run_id)) is not None
        reuse = await store.get_reuse_decision("tenant-1", receipt.target_run_id, draft_key)
        assert reuse is not None and reuse.candidate is not None
        assert reuse.candidate.result_manifest_ref == "s3://technical/draft/manifest"
        async with runtime.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            row = await connection.fetchrow(
                """
                SELECT status, schema_version, source_run_id, source_snapshot_id,
                       patch_digest, target_run_id, source_binding_id
                FROM belllabs_control.runtime_fork_requests WHERE request_id = $1
                """,
                receipt.request_id,
            )
            assert row is not None
            assert dict(row) == {
                "status": "accepted",
                "schema_version": "belllabs.run-fork-request.v2",
                "source_run_id": source,
                "source_snapshot_id": snapshot.snapshot_id,
                "patch_digest": receipt.patch_digest,
                "target_run_id": receipt.target_run_id,
                "source_binding_id": None,
            }
            edges = await connection.fetch(
                """
                SELECT relationship FROM belllabs_control.runtime_lineage_edges
                WHERE lineage_id = $1 ORDER BY relationship
                """,
                f"fork-lineage:{receipt.request_id}",
            )
            assert [item["relationship"] for item in edges] == [
                "contains",
                "derived_from",
                "reuses",
            ]
            for statement in (
                "UPDATE belllabs_control.run_fork_reuse_decisions SET reason = 'x'",
                "DELETE FROM belllabs_control.run_snapshot_manifests",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)

        # The read-only role reads the fork authority in its scope and writes nothing.
        async with readonly.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            counts = await connection.fetchrow(
                """
                SELECT
                  (SELECT count(*) FROM belllabs_control.run_snapshot_manifests) AS snapshots,
                  (SELECT count(*) FROM belllabs_control.run_fork_reuse_decisions) AS decisions,
                  (SELECT count(*) FROM belllabs_control.runtime_fork_requests) AS forks
                """
            )
            assert counts is not None and tuple(counts) == (1, 2, 1)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(
                        "DELETE FROM belllabs_control.run_fork_reuse_decisions"
                    )
        async with readonly.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.run_fork_reuse_decisions"
                )
                == 0
            )

        # A different materialization for the same fork is a conflict, never applied.
        request = await fork_repository.get_request("tenant-1", receipt.request_id)
        assert request is not None
        tampered = ForkLineageManifest.create(
            **receipt.lineage.model_dump(mode="python", exclude={"lineage_digest"})
            | {"invalidated_unit_keys": ()}
        )
        with pytest.raises(IdempotencyConflict):
            await store.record_materialization(request, tampered, _lineage_stub(request))

        # An audited fork retention purge removes the fork with its decisions.
        deleted = await PostgresStage3RetentionRepository(runtime, AllowRetention()).delete_expired(
            request_scope="tenant-1",
            record_class="fork",
            cutoff_at=NOW + timedelta(days=365),
            actor_id="operator:retention",
            reason="audited fork purge",
            deletion_id="deletion-fork-1",
            recorded_at=NOW + timedelta(days=365),
        )
        assert deleted == 1
        assert await store.list_reuse_decisions("tenant-1", receipt.request_id) == ()
    finally:
        for pool in (runtime, readonly):
            if pool is not None:
                await pool.close()
        await owner.close()


def _lineage_stub(request: Any) -> Any:
    from app.application.runtime.run_forks import fork_execution_lineage
    from app.domain.run_control.forks import lineage_for

    return fork_execution_lineage(
        request,
        lineage_for(request, admission_ref="admission:stub"),
        retain_until=request.requested_at + timedelta(days=90),
    )


@pytest.mark.asyncio
async def test_fork_source_reader_reads_family_heads_and_linked_runs(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime: asyncpg.Pool | None = None
    try:
        await reset_application_schema(owner)
        run_control, _ = run_control_service(PostgresRunControlRepository(owner))  # type: ignore[arg-type]
        parent = await run_control.admit(run_request(request_id="rrm006-parent"))
        child = await run_control.admit(run_request(request_id="rrm006-child"))
        assert parent.run_id is not None and child.run_id is not None
        child_budget = await run_control.get_budget("tenant-1", child.run_id)
        mutation = {"family_kind": "stagegraph", "mutation_id": "result-head"}
        async with owner.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO belllabs_control.family_admission_heads (
                    request_scope, run_id, family_kind, family_version,
                    mutation_fingerprint, mutation, updated_at
                ) VALUES ('tenant-1', $1, 'stagegraph', 3, $2, $3::jsonb, $4)
                """,
                parent.run_id,
                sha256_digest(mutation),
                '{"family_kind": "stagegraph", "mutation_id": "result-head"}',
                NOW,
            )
            await connection.execute(
                """
                INSERT INTO belllabs_control.run_composition_links (
                    link_id, request_identity, request_fingerprint, request_scope,
                    parent_run_id, child_run_id, linked_budget_account_id, link, created_at
                ) VALUES ('link-1', 'link-request-1', $1, 'tenant-1', $2, $3, $4, '{}'::jsonb, $5)
                """,
                sha256_digest("link"),
                parent.run_id,
                child.run_id,
                child_budget.account_id,
                NOW,
            )
        runtime = await asyncpg.create_pool(
            dsn=test_application_postgres_dsn, min_size=1, max_size=2, setup=_assume(RUNTIME_ROLE)
        )
        reader = PostgresForkSourceReader(runtime)
        facts = await reader.read_fork_source("tenant-1", parent.run_id)
        assert facts is not None
        assert facts.run_version == 1
        assert [(head.family_kind, head.family_version) for head in facts.family_heads] == [
            ("stagegraph", 3)
        ]
        assert facts.family_heads[0].mutation["mutation_id"] == "result-head"
        assert [(item.link_id, item.terminal_status) for item in facts.linked_runs] == [
            ("link-1", None)
        ]
        assert facts.blueprint_digest is None
        assert await reader.read_fork_source("tenant-2", parent.run_id) is None
    finally:
        if runtime is not None:
            await runtime.close()
        await owner.close()
