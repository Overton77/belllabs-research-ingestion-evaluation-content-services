"""RRM-006 on mission_control: least privilege, RLS, and fork crash recovery.

Every fork write runs as a restricted login of `mission_control_runtime` (no superuser, no
`BYPASSRLS`); reads are re-checked under `mission_control_readonly`. Sealed snapshots are
canonical continuation checkpoints, forks canonical recovery requests (kind 'fork') with a
fork_lineage once accepted. The saga is driven through run-control admission with injected
crashes after the admission commit and after the materialization commit: each retry
reconciles from PostgreSQL and the fork ends with exactly one durable receipt, one admitted
derived run, and one set of reuse decisions.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest

from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.runtime.run_forks import (
    PostgresForkMaterializationStore,
    PostgresForkSourceReader,
    PostgresRunSnapshotRepository,
)
from mission_control.adapters.postgres.runtime.stage3_kernel_repository import (
    PostgresForkRepository,
)
from mission_control.adapters.postgres.scope import apply_scope, scope_values
from mission_control.application.recovery.run_forks import (
    ForkPatchPolicyRegistry,
    RecordingForkMaterializer,
    RunControlForkAuthority,
    SemanticForkService,
)
from mission_control.application.recovery.runtime_recovery import RuntimeForkService
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.policies.contracts import RunPhase, StartAction
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import (
    ForkLineageManifest,
    ForkRejected,
    ReuseCandidate,
    derived_unit_identity,
)
from tests.fixtures.checkpoint_recovery import stage_recovery_unit
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.run_forks import (
    fork_command,
    review_objective_patch,
    stage_policy,
    technical_snapshot,
)
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import (
    owner_rows,
    scoped_command,
    scoped_request,
)
from tests.unit.run_control.test_run_control import NOW, WORKFLOW_DIGEST
from tests.unit.run_control.test_run_control import service as run_control_service

pytestmark = pytest.mark.common_db

RUNTIME_ROLE = "mission_control_runtime"
READONLY_ROLE = "mission_control_readonly"
NEW_TABLES = ("run_snapshot", "fork_reuse_decision", "fork_request")
_BOUND: list[CommonDatabase] = []


@pytest.fixture(autouse=True)
def _bind_database(common_db: CommonDatabase):  # type: ignore[no-untyped-def]
    _BOUND.append(common_db)
    yield
    _BOUND.remove(common_db)


def scope(tenant: str = "tenant-1") -> str:
    return _BOUND[-1].scope(tenant)


def recovery_unit(run_id: str, name: str = "draft"):  # type: ignore[no-untyped-def]
    return stage_recovery_unit(run_id, name, request_scope=scope())


def run_request(**kwargs: Any):  # type: ignore[no-untyped-def]
    return scoped_request(_BOUND[-1], **kwargs)


def command(run_id: str, version: int, command_id: str, action: object):  # type: ignore[no-untyped-def]
    return scoped_command(_BOUND[-1], run_id, version, command_id, action)


async def _insert_head(
    db: CommonDatabase, run_id: str, version: int, fingerprint: str, mutation: str
) -> None:
    await owner_rows(
        db,
        """
        INSERT INTO mission_control.family_admission_head (
            installation_id, application_id, tenant_id, family_admission_head_id, run_key,
            family_kind, family_version, mutation_fingerprint, mutation_contract, mutation,
            updated_at, created_at, created_by_actor_ref
        ) VALUES ($1, $2, $3, gen_random_uuid(), $4, 'stagegraph', $5, $6,
                  'mc.family-mutation/1', $7::jsonb, $8, $8, 'fixture')
        """,
        *scope_values(parse_request_scope(db.scope())),
        run_id,
        version,
        fingerprint,
        mutation,
        NOW,
    )


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
    unit = recovery_unit(run_id, name)
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
async def test_fork_support_records_force_rls_and_least_privilege(
    common_db: CommonDatabase,
) -> None:
    for table in NEW_TABLES:
        rows = await owner_rows(
            common_db,
            """
            SELECT relrowsecurity, relforcerowsecurity FROM pg_class
            WHERE oid = ('mission_control.' || $1)::regclass
            """,
            table,
        )
        assert tuple(rows[0]) == (True, True)
        qualified = f"mission_control.{table}"
        privileges = {}
        for role in (RUNTIME_ROLE, READONLY_ROLE):
            for action in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                privileges[(role, action)] = (
                    await owner_rows(
                        common_db,
                        "SELECT has_table_privilege($1, $2, $3) AS allowed",
                        role,
                        qualified,
                        action,
                    )
                )[0]["allowed"]
        assert privileges == {
            (RUNTIME_ROLE, "SELECT"): True,
            (RUNTIME_ROLE, "INSERT"): True,
            (RUNTIME_ROLE, "UPDATE"): table == "fork_request",
            (RUNTIME_ROLE, "DELETE"): False,
            (READONLY_ROLE, "SELECT"): True,
            (READONLY_ROLE, "INSERT"): False,
            (READONLY_ROLE, "UPDATE"): False,
            (READONLY_ROLE, "DELETE"): False,
        }
    relationship = (
        await owner_rows(
            common_db,
            """
            SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint
            WHERE conrelid = 'mission_control.execution_lineage_edge'::regclass
              AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%relationship%'
            """,
        )
    )[0]["definition"]
    for value in ("derived_from", "seeded_from", "reuses", "contains", "claims"):
        assert f"'{value}'" in relationship


@pytest.mark.asyncio
async def test_fork_saga_recovers_crashes_under_the_runtime_role_with_one_receipt(
    common_db: CommonDatabase,
) -> None:
    runtime: asyncpg.Pool | None = None
    readonly: asyncpg.Pool | None = None
    try:
        runtime = await common_db.pool(max_size=6)
        readonly = await common_db.pool(READONLY_ROLE, max_size=2)
        async with runtime.acquire() as connection:
            assert await connection.fetchval(
                "SELECT pg_has_role(current_user, $1, 'MEMBER')", RUNTIME_ROLE
            )
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
            request_scope=scope(),
            candidates=(_candidate(source, "draft"), _candidate(source, "review")),
        )
        snapshots = PostgresRunSnapshotRepository(runtime)
        assert await snapshots.put(snapshot) == snapshot
        assert await snapshots.put(snapshot) == snapshot  # idempotent
        conflicting = technical_snapshot(source, request_scope=scope()).model_copy(
            update={"taken_at": NOW}
        )
        with pytest.raises(ForkRejected) as conflict:
            await snapshots.put(conflicting)
        assert conflict.value.code == "snapshot_digest_conflict"
        assert await snapshots.get(scope("tenant-2"), snapshot.snapshot_id) is None

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
        derived = await run_control.get_run(scope(), receipt.target_run_id)
        assert (derived.phase, derived.version) == (RunPhase.PENDING, 1)
        draft_key = derived_unit_identity(
            recovery_unit(source, "draft"), receipt.target_run_id
        ).unit_key
        review_key = derived_unit_identity(
            recovery_unit(source, "review"), receipt.target_run_id
        ).unit_key
        decisions = await store.list_reuse_decisions(scope(), receipt.request_id)
        assert {(item.derived_unit_key, item.decision) for item in decisions} == {
            (draft_key, "reuse"),
            (review_key, "invalidated"),
        }
        assert receipt.lineage.reused_unit_keys == (draft_key,)
        assert (await store.fork_of_run(scope(), receipt.target_run_id)) is not None
        reuse = await store.get_reuse_decision(scope(), receipt.target_run_id, draft_key)
        assert reuse is not None and reuse.candidate is not None
        assert reuse.candidate.result_manifest_ref == "s3://technical/draft/manifest"
        async with runtime.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope())
            row = await connection.fetchrow(
                """
                SELECT f.status, f.schema_version, f.source_run_key, f.source_snapshot_key,
                       f.patch_digest, f.target_run_key, r.kind, r.state,
                       target.run_key AS recovery_target
                FROM mission_control.fork_request f
                JOIN mission_control.recovery_request r
                  USING (installation_id, application_id, tenant_id, recovery_id)
                JOIN mission_control.mission_run target
                  ON target.installation_id = r.installation_id
                 AND target.application_id = r.application_id
                 AND target.tenant_id = r.tenant_id AND target.run_id = r.target_run_id
                WHERE f.request_key = $1
                """,
                receipt.request_id,
            )
            assert row is not None
            assert dict(row) == {
                "status": "accepted",
                "schema_version": "belllabs.run-fork-request.v2",
                "source_run_key": source,
                "source_snapshot_key": snapshot.snapshot_id,
                "patch_digest": receipt.patch_digest,
                "target_run_key": receipt.target_run_id,
                "kind": "fork",
                "state": "completed",
                "recovery_target": receipt.target_run_id,
            }
            lineage = await connection.fetchrow(
                """
                SELECT source.run_key AS source_run, target.run_key AS target_run,
                       l.source_checkpoint_digest, c.checkpoint_key
                FROM mission_control.fork_lineage l
                JOIN mission_control.mission_run source
                  ON source.installation_id = l.installation_id
                 AND source.application_id = l.application_id
                 AND source.tenant_id = l.tenant_id AND source.run_id = l.source_run_id
                JOIN mission_control.mission_run target
                  ON target.installation_id = l.installation_id
                 AND target.application_id = l.application_id
                 AND target.tenant_id = l.tenant_id AND target.run_id = l.target_run_id
                JOIN mission_control.continuation_checkpoint c
                  ON c.installation_id = l.installation_id
                 AND c.application_id = l.application_id
                 AND c.tenant_id = l.tenant_id AND c.checkpoint_id = l.source_checkpoint_id
                """
            )
            assert lineage is not None and dict(lineage) == {
                "source_run": source,
                "target_run": receipt.target_run_id,
                "source_checkpoint_digest": snapshot.snapshot_digest,
                "checkpoint_key": snapshot.snapshot_id,
            }
            edges = await connection.fetch(
                """
                SELECT relationship FROM mission_control.execution_lineage_edge
                WHERE lineage_key = $1 ORDER BY relationship
                """,
                f"fork-lineage:{receipt.request_id}",
            )
            assert [item["relationship"] for item in edges] == [
                "contains",
                "derived_from",
                "reuses",
            ]
            for statement in (
                "UPDATE mission_control.fork_reuse_decision SET reason = 'x'",
                "DELETE FROM mission_control.run_snapshot",
                "UPDATE mission_control.continuation_checkpoint SET manifest_ref = 'x'",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)

        # The read-only role reads the fork authority in its scope and writes nothing.
        async with readonly.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope())
            counts = await connection.fetchrow(
                """
                SELECT
                  (SELECT count(*) FROM mission_control.run_snapshot) AS snapshots,
                  (SELECT count(*) FROM mission_control.fork_reuse_decision) AS decisions,
                  (SELECT count(*) FROM mission_control.fork_request) AS forks
                """
            )
            assert counts is not None and tuple(counts) == (1, 2, 1)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute("DELETE FROM mission_control.fork_reuse_decision")
        async with readonly.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope("tenant-2"))
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.fork_reuse_decision"
                )
                == 0
            )

        # A different materialization for the same fork is a conflict, never applied.
        request = await fork_repository.get_request(scope(), receipt.request_id)
        assert request is not None
        tampered = ForkLineageManifest.create(
            **receipt.lineage.model_dump(mode="python", exclude={"lineage_digest"})
            | {"invalidated_unit_keys": ()}
        )
        with pytest.raises(IdempotencyConflict):
            await store.record_materialization(request, tampered, _lineage_stub(request))

        # The retention purge path is retired with its tests-only repository: the fork,
        # its decisions and its lineage stay; the derived run names its fork in its
        # admission transition.
        assert await store.fork_marker(scope(), receipt.target_run_id) == receipt.request_id
        assert await store.fork_marker(scope(), source) is None
    finally:
        for pool in (runtime, readonly):
            if pool is not None:
                await pool.close()


def _lineage_stub(request: Any) -> Any:
    from mission_control.application.recovery.run_forks import fork_execution_lineage
    from mission_control.domain.policies.forks import lineage_for

    return fork_execution_lineage(
        request,
        lineage_for(request, admission_ref="admission:stub"),
        retain_until=request.requested_at + timedelta(days=90),
    )


@pytest.mark.asyncio
async def test_fork_source_reader_reads_family_heads_and_linked_runs(
    common_db: CommonDatabase,
) -> None:
    owner = await common_db.pool(max_size=2)
    runtime: asyncpg.Pool | None = None
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(owner))  # type: ignore[arg-type]
        parent = await run_control.admit(run_request(request_id="rrm006-parent"))
        child = await run_control.admit(run_request(request_id="rrm006-child"))
        assert parent.run_id is not None and child.run_id is not None
        child_budget = await run_control.get_budget(scope(), child.run_id)
        mutation = {"family_kind": "stagegraph", "mutation_id": "result-head"}
        await _insert_head(
            common_db,
            parent.run_id,
            3,
            sha256_digest(mutation),
            '{"family_kind": "stagegraph", "mutation_id": "result-head"}',
        )
        # The link goes through the linked-run repository (canonical mission_relationship
        # plus its support record); the reader sees it with no terminal yet.
        from mission_control.adapters.postgres.orchestration.linked_run_repository import (
            PostgresLinkedRunRepository,
        )
        from mission_control.domain.composition.contracts import (
            RunCompositionLink,
            RunDependencyClass,
        )

        await PostgresLinkedRunRepository(owner).commit_link(
            RunCompositionLink(
                link_id="link-1",
                request_identity="link-request-1",
                request_fingerprint=sha256_digest("link"),
                request_scope=scope(),
                parent_run_id=parent.run_id,
                child_run_id=child.run_id,
                slot_id="child_work",
                request_revision=1,
                target_workflow_type_ref=run_request().workflow_type_ref,
                child_effective_configuration_digest=run_request().effective_configuration_digest,
                dependency_class=RunDependencyClass.REQUIRED_BLOCKING,
                linked_budget_account_id=child_budget.account_id,
                result_admission_policy="linked-result:exact@1",
                cancellation_policy="request_cancel",
                created_at=NOW,
            )
        )
        runtime = await common_db.pool(max_size=2)
        reader = PostgresForkSourceReader(runtime)
        facts = await reader.read_fork_source(scope(), parent.run_id)
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
        assert await reader.read_fork_source(scope("tenant-2"), parent.run_id) is None
    finally:
        if runtime is not None:
            await runtime.close()
        await owner.close()


@pytest.mark.asyncio
async def test_fork_requests_reject_uniqueness_scope_and_snapshot_updates(
    common_db: CommonDatabase,
) -> None:
    owner = await common_db.pool(max_size=2)
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(owner))  # type: ignore[arg-type]
        admitted = await run_control.admit(run_request(request_id="rrm006-violations"))
        assert admitted.run_id is not None
        source = admitted.run_id
        snapshot = technical_snapshot(source, request_scope=scope())
        await PostgresRunSnapshotRepository(owner).put(snapshot)
        insert = """
            WITH recovery AS (
                INSERT INTO mission_control.recovery_request (
                    installation_id, application_id, tenant_id, recovery_id, source_run_id,
                    source_checkpoint_id, kind, actor_ref, action, request_key, payload_digest,
                    state, detail, version, updated_at, created_at, created_by_actor_ref
                )
                SELECT $1, $2, $3, gen_random_uuid(), run.run_id, NULL, 'fork', 'fixture',
                       'mc.run.fork', $4, $5, 'admitted', '{}'::jsonb, 1, $6, $6, 'fixture'
                FROM mission_control.mission_run run WHERE run.run_key = $7
                RETURNING recovery_id
            )
            INSERT INTO mission_control.fork_request (
                installation_id, application_id, tenant_id, fork_request_id, request_key,
                recovery_id, idempotency_key, schema_version, request_digest, request_payload,
                status, source_run_key, source_snapshot_key, patch_digest, target_run_key,
                requested_at, updated_at, retain_until, created_at, created_by_actor_ref
            )
            SELECT $1, $2, $3, gen_random_uuid(), $4, recovery_id, $4,
                   'belllabs.run-fork-request.v2', $5, '{}'::jsonb, 'reserved', $7, $8, $5, $9,
                   $6, $6, $6 + interval '1 day', $6, 'fixture'
            FROM recovery
        """
        tenant_1 = scope_values(parse_request_scope(scope()))
        tenant_2 = scope_values(parse_request_scope(scope("tenant-2")))
        window = (NOW, source, snapshot.snapshot_id)
        connection = await asyncpg.connect(common_db.owner_dsn)
        try:
            await connection.execute(
                insert,
                *tenant_1,
                "fork-b",
                sha256_digest("b"),
                window[0],
                source,
                snapshot.snapshot_id,
                "derived-b",
            )
            # One fork per derived run.
            with pytest.raises(asyncpg.UniqueViolationError, match="target_run"):
                await connection.execute(
                    insert,
                    *tenant_1,
                    "fork-c",
                    sha256_digest("c"),
                    window[0],
                    source,
                    snapshot.snapshot_id,
                    "derived-b",
                )
        finally:
            await connection.close()
        async with owner.acquire() as connection:
            async with connection.transaction():
                await apply_scope(connection, scope())
                # Forced RLS WITH CHECK: a row for another tenant cannot be inserted.
                with pytest.raises(asyncpg.InsufficientPrivilegeError, match="row-level security"):
                    await connection.execute(
                        "INSERT INTO mission_control.fork_reuse_decision (installation_id,"
                        " application_id, tenant_id, fork_reuse_decision_id, fork_request_key,"
                        " derived_run_key, derived_unit_key, source_run_key, source_unit_key,"
                        " schema_version, decision, reason, decision_digest, decision_payload,"
                        " recorded_at, created_at, created_by_actor_ref) VALUES ($1, $2, $3,"
                        " gen_random_uuid(), 'fork-b', 'x', $4, 'y', $4,"
                        " 'belllabs.fork-reuse-decision.v1', 'excluded', 'r', $5, '{}'::jsonb,"
                        " now(), now(), 'x')",
                        *tenant_2,
                        "bl-unit-v1:" + "0" * 64,
                        sha256_digest("d"),
                    )
            async with connection.transaction():
                await apply_scope(connection, scope())
                # Sealed snapshots are immutable for the runtime role.
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(
                        "UPDATE mission_control.continuation_checkpoint SET created_at = created_at"
                    )
            async with connection.transaction():
                await apply_scope(connection, scope())
                visible = await connection.fetchval(
                    "SELECT count(*) FROM mission_control.fork_request"
                )
                assert visible == 1
    finally:
        await owner.close()


@pytest.mark.asyncio
async def test_active_async_child_in_rrm013_authority_blocks_the_snapshot(
    common_db: CommonDatabase,
) -> None:
    """The production classifier: RRM-013's `classify_async_children_for_fork` over
    `PostgresAsyncSubagentAuthority.list_children` (0016 authority, 0021 lifecycle)."""

    from mission_control.adapters.postgres.async_subagents.async_subagents import (
        PostgresAsyncSubagentAuthority,
    )
    from mission_control.adapters.postgres.run_control.inspection_repository import (
        PostgresInspectionReadRepository,
    )
    from mission_control.application.recovery.run_forks import (
        LedgerPendingCommands,
        LineageAsyncChildForkClassifier,
        RunSnapshotService,
    )
    from mission_control.domain.policies.contracts import StartAction
    from tests.acceptance.control_plane.test_wp_cp_045 import request as spawn_request
    from tests.fixtures.run_forks import stagegraph_head

    owner = await common_db.pool(max_size=4)
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(owner))  # type: ignore[arg-type]
        base = run_request(request_id="rrm006-async-child")
        admitted = await run_control.admit(
            base.model_copy(
                update={
                    "budget_envelope": base.budget_envelope.model_copy(
                        update={"baseline_reservations": {}}
                    )
                }
            )
        )
        assert admitted.run_id is not None
        run_id = admitted.run_id
        await run_control.execute(command(run_id, 1, "rrm006-async-child-start", StartAction()))
        head = stagegraph_head(stages={"draft": "completed"})
        await _insert_head(
            common_db,
            run_id,
            head.family_version,
            head.mutation_fingerprint,
            json.dumps(dict(head.mutation)),
        )
        authority = PostgresAsyncSubagentAuthority(owner)
        service = RunSnapshotService(
            reads=PostgresInspectionReadRepository(owner),
            sources=PostgresForkSourceReader(owner),
            snapshots=PostgresRunSnapshotRepository(owner),
            async_children=LineageAsyncChildForkClassifier(authority),
            commands=LedgerPendingCommands(run_control),
        )
        await authority.reserve_and_admit(
            spawn_request().model_copy(update={"request_scope": scope(), "parent_run_id": run_id}),
            "async-child-admitted",
            "link-admitted",
        )
        with pytest.raises(ForkRejected) as rejected:
            await service.take(scope(), run_id)
        assert rejected.value.code == "snapshot_not_quiescent"
        assert rejected.value.reasons == ("async_child_active:async-child-admitted",)

        await owner_rows(
            common_db,
            "UPDATE mission_control.subordinate_admission SET lifecycle = 'completed'"
            " WHERE subordinate_key = 'async-child-admitted'",
        )
        snapshot = await service.take(scope(), run_id)
        observed = [
            (item.child_execution_id, item.lifecycle, item.disposition)
            for item in snapshot.async_children
        ]
        assert observed == [("async-child-admitted", "completed", "terminal")]

        # RRM-007's durable ledger (0023): an accepted, unapplied pause on a run with a family
        # execution target is a pending command; once applied the snapshot is taken.
        from tests.unit.run_control.test_boundary_commands import (
            TARGET,
            apply,
            boundary_command,
            pause,
        )

        targeted = await run_control.admit(run_request(request_id="rrm006-ledger"))
        assert targeted.run_id is not None
        ledger_run = targeted.run_id
        await run_control.execute(
            command(ledger_run, 1, "ledger-start", StartAction(execution_target=TARGET))
        )
        await _insert_head(
            common_db,
            ledger_run,
            head.family_version,
            head.mutation_fingerprint,
            json.dumps(dict(head.mutation)),
        )
        accepted = await run_control.execute(command(ledger_run, 2, "pause", pause()))
        assert accepted.reason_code == "accepted_pending_application"
        with pytest.raises(ForkRejected) as pending:
            await service.take(scope(), ledger_run)
        assert "command_unapplied:operator:pause:accepted" in pending.value.reasons
        await run_control.execute(
            boundary_command(ledger_run, 2, "apply:pause", apply("pause", pause())).model_copy(
                update={"request_scope": scope()}
            )
        )
        assert (await service.take(scope(), ledger_run)).pending_commands == ()
    finally:
        await owner.close()
