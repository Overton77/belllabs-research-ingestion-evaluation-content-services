"""Durable runtime kernel persistence on mission_control (production paths only).

Runtime bindings (canonical execution_binding + support status), bootstrap authority and
reconciliation decisions (canonical human_task/human_resolution), sealed snapshots and the
fork saga (canonical continuation_checkpoint, recovery_request and fork_lineage) and the
immutable execution-lineage journal, all under a restricted `mission_control_runtime` login
with forced RLS. The tests-only lease journal, incident repair and retention deletion
repositories were retired with their legacy tables.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.runtime.run_forks import PostgresRunSnapshotRepository
from mission_control.adapters.postgres.runtime.runtime_authority import (
    PostgresBootstrapAuthority,
    PostgresBootstrapDecisionBridge,
)
from mission_control.adapters.postgres.runtime.runtime_execution_repository import (
    PostgresRuntimeCoordinationRepository,
)
from mission_control.adapters.postgres.runtime.stage3_kernel_repository import (
    RETENTION_DAYS,
    PostgresDecisionRepository,
    PostgresForkRepository,
    append_lineage_in_transaction,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.recovery.runtime_bootstrap import BootstrapRequest
from mission_control.application.recovery.runtime_lineage import PersistedExecutionLineage
from mission_control.application.recovery.runtime_recovery import ForkAdmission
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.graph_runtime.contracts import (
    ActorRef,
    Correlation,
    GraphExecutionSubmission,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from mission_control.domain.graph_runtime.definitions import (
    ContentAddressedRef,
    ExecutionLineageEnvelope,
    RuntimeDefinitionKind,
)
from mission_control.domain.graph_runtime.identities import (
    ExecutionEpochKey,
)
from mission_control.domain.graph_runtime.kernel import (
    DecisionRequest,
    DecisionResponse,
    LineageKind,
    LineageParentEdge,
    ProviderQualifiedLineageRecord,
)
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import (
    RunForkPatch,
    RunForkReceipt,
    RunForkRequest,
    admission_request_ref,
    lineage_for,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.run_forks import technical_snapshot
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows, scoped_request
from tests.unit.run_control.test_run_control import service as run_control_service

pytestmark = pytest.mark.common_db

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


async def _admit_run(pool: asyncpg.Pool, db: CommonDatabase, *, tenant: str = "tenant-1") -> str:
    run_service, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    decision = await run_service.admit(scoped_request(db, tenant))
    assert decision.run_id is not None
    return decision.run_id


async def _create_binding(
    pool: asyncpg.Pool,
    *,
    request_scope: str,
    run_id: str,
    binding_id: str = "binding-1",
) -> RuntimeExecutionBinding:
    epoch = ExecutionEpochKey(
        request_scope=request_scope,
        belllabs_run_id=run_id,
        execution_epoch=1,
    )
    values = {
        "submission_id": f"submission-{binding_id}",
        "idempotency_key": f"idempotency-{binding_id}",
        "epoch": epoch,
        "expected_belllabs_version": 1,
        "run_plan_ref": ContentAddressedRef(
            kind=RuntimeDefinitionKind.RUN_PLAN,
            logical_id="run-plan",
            schema_version="1",
            digest=DIGEST_A,
        ),
        "run_plan_digest": DIGEST_A,
        "graph_assembly_digest": DIGEST_A,
        "target_deployment": None,
        "target_graph_id": None,
        "state_schema_digest": DIGEST_A,
        "input_manifest_ref": "input-manifest-1",
        "actor": ActorRef(
            actor_id="runtime-dispatch",
            actor_type="service",
            authority_ref="authority:runtime-dispatch@1",
        ),
        "correlation": Correlation(correlation_id=f"correlation-{binding_id}"),
        "submitted_at": NOW,
    }
    submission = GraphExecutionSubmission(
        **values,
        request_digest=sha256_digest(values),
    )
    binding = RuntimeExecutionBinding(
        binding_id=binding_id,
        epoch=epoch,
        submission_id=submission.submission_id,
        submission_idempotency_key=submission.idempotency_key,
        submission_digest=submission.request_digest,
        run_plan_digest=submission.run_plan_digest,
        graph_assembly_digest=submission.graph_assembly_digest,
        state_schema_digest=submission.state_schema_digest,
        runtime_provider="legacy_temporal",
        status=RuntimeExecutionStatus.SUBMITTING,
        created_at=NOW,
        updated_at=NOW,
    )
    reservation = await PostgresRuntimeCoordinationRepository(pool).create_binding(
        submission,
        binding,
    )
    return reservation.binding


def _lineage(
    *,
    lineage_id: str,
    run_id: str,
    runtime_attempt_id: str,
    request_scope: str,
    parent_lineage_id: str | None = None,
    result_manifest_ref: str | None = None,
    recorded_at: datetime = NOW,
) -> PersistedExecutionLineage:
    envelope = ExecutionLineageEnvelope(
        request_scope=request_scope,
        belllabs_run_id=run_id,
        execution_epoch=1,
        workflow_implementation_ref="workflow-implementation:1",
        graph_assembly_digest=DIGEST_A,
        workflow_cycle=0,
        stage_id="selection",
        stage_cycle=0,
        semantic_operation_attempt_id="semantic-attempt-1",
        runtime_attempt_id=runtime_attempt_id,
        operation_binding_id="operation-binding-1",
        operation_assembly_digest=DIGEST_A,
        parent_lineage_id=parent_lineage_id,
        result_manifest_ref=result_manifest_ref,
        evidence_refs=("evidence:1",),
        usage_settlement_refs=("settlement:1",),
    )
    run_identity = ProviderQualifiedLineageRecord(
        kind=LineageKind.BELL_LABS_RUN,
        provider="belllabs",
        provider_identity=run_id,
        request_scope=request_scope,
        canonical_digest=DIGEST_A,
    )
    attempt_identity = ProviderQualifiedLineageRecord(
        kind=LineageKind.RUNTIME_ATTEMPT,
        provider="belllabs",
        provider_identity=runtime_attempt_id,
        request_scope=request_scope,
        canonical_digest=DIGEST_A,
    )
    return PersistedExecutionLineage.create(
        lineage_id=lineage_id,
        envelope=envelope,
        qualified_identities=(run_identity, attempt_identity),
        parent_edges=(
            LineageParentEdge(
                child=attempt_identity,
                parent=run_identity,
                relationship="attempt_of",
            ),
        ),
        recorded_at=recorded_at,
        retain_until=recorded_at + timedelta(days=RETENTION_DAYS),
    )


def _decision_request(
    *,
    binding_id: str,
    request_scope: str,
    decision_id: str = "decision-1",
) -> DecisionRequest:
    values = {
        "decision_id": decision_id,
        "request_scope": request_scope,
        "binding_id": binding_id,
        "decision_type": "approval",
        "schema_ref": "schema:approval:1",
        "choices_ref": "choices:approval:1",
        "evidence_refs": ("evidence:1",),
        "expected_lifecycle_version": 7,
        "policy_ref": "policy:approval:1",
        "requested_at": NOW,
        "expires_at": NOW + timedelta(hours=1),
    }
    return DecisionRequest(**values, request_digest=sha256_digest(values))


def _decision_response(
    *,
    decision_id: str = "decision-1",
    request_scope: str,
    response_digest: str = DIGEST_A,
) -> DecisionResponse:
    return DecisionResponse(
        decision_id=decision_id,
        request_scope=request_scope,
        response_id=f"response-{decision_id}",
        response_schema_ref="schema:approval:1",
        response_payload_ref=f"response-payload:{decision_id}",
        response_digest=response_digest,
        expected_lifecycle_version=7,
        actor_ref="operator:1",
        decided_at=NOW + timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_stage3_kernel_postgres_persistence_slice(common_db: CommonDatabase) -> None:
    db = common_db
    tenant_1, tenant_2 = db.scope("tenant-1"), db.scope("tenant-2")
    pool = await db.pool(max_size=6)
    try:
        run_id = await _admit_run(pool, db)
        tenant_two_run = await _admit_run(pool, db, tenant="tenant-2")
        binding = await _create_binding(
            pool,
            request_scope=tenant_1,
            run_id=run_id,
            binding_id="binding-1",
        )
        bootstrap_projection = await PostgresBootstrapAuthority(pool).load(binding.epoch)
        assert bootstrap_projection.binding == binding
        bootstrap_request = BootstrapRequest(
            epoch=binding.epoch,
            runtime_binding_ref=binding.binding_id,
            run_plan_digest=binding.run_plan_digest,
            graph_assembly_digest=binding.graph_assembly_digest,
            state_schema_digest=binding.state_schema_digest,
        )
        bootstrap_decision_id = await PostgresBootstrapDecisionBridge(
            pool
        ).persist_reconciliation_decision(
            bootstrap_request,
            bootstrap_projection,
            "checkpoint_route_incompatible",
        )
        assert bootstrap_decision_id.startswith("decision-")
        # The admitted runtime binding is an immutable canonical execution binding of the run.
        bindings = await owner_rows(
            db,
            """
            SELECT e.binding_contract, s.status, r.run_key
            FROM mission_control.runtime_execution_binding s
            JOIN mission_control.execution_binding e
              USING (installation_id, application_id, tenant_id, execution_binding_id)
            JOIN mission_control.mission_run r
              ON r.installation_id = e.installation_id AND r.application_id = e.application_id
             AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
            WHERE s.binding_key = 'binding-1'
            """,
        )
        assert [tuple(row.values()) for row in bindings] == [
            ("mc.runtime-execution-binding/1", "submitting", run_id)
        ]
        # RRM-006: the versioned fork saga state (snapshot, patch, derived run).
        snapshot = technical_snapshot(run_id, request_scope=tenant_1)
        await PostgresRunSnapshotRepository(pool).put(snapshot)
        fork_target = scoped_request(db, request_id="fork-1")
        fork_request = RunForkRequest(
            request_id="fork-1",
            idempotency_key="fork-1",
            request_scope=tenant_1,
            source_run_id=run_id,
            source_execution_epoch=1,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.snapshot_digest,
            patch=RunForkPatch.create(
                source_snapshot_id=snapshot.snapshot_id,
                source_snapshot_digest=snapshot.snapshot_digest,
                target_admission_request_ref=admission_request_ref(fork_target),
            ),
            target=fork_target,
            derived_run_id="run-fork-1",
            actor_id="operator",
            reason="durable recovery fork",
            requested_at=NOW,
        )
        fork_repo = PostgresForkRepository(pool)
        assert await fork_repo.reserve(fork_request) is True
        assert await fork_repo.reserve(fork_request) is False
        assert await fork_repo.get_request(tenant_1, "fork-1") == fork_request
        assert await fork_repo.claim_admission(fork_request) is True
        assert await fork_repo.claim_admission(fork_request) is False
        fork_admission = ForkAdmission(
            request_id="fork-1",
            target_epoch=ExecutionEpochKey(
                request_scope=tenant_1,
                belllabs_run_id="run-fork-1",
                execution_epoch=1,
            ),
            admission_ref="admission:tenant-1:operator:fork-1",
            budget_reservation_ref="budget:fork-1",
            admitted_effective_configuration_digest=fork_target.effective_configuration_digest,
        )
        assert await fork_repo.record_admission(fork_request, fork_admission) == fork_admission
        assert await fork_repo.claim_copy(fork_request) is True
        assert await fork_repo.claim_copy(fork_request) is False
        fork_receipt = RunForkReceipt(
            request_id="fork-1",
            request_scope=tenant_1,
            source_run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            snapshot_digest=snapshot.snapshot_digest,
            patch_digest=fork_request.patch.patch_digest,
            target_run_id="run-fork-1",
            admission_ref=fork_admission.admission_ref,
            admitted_effective_configuration_digest=(
                fork_admission.admitted_effective_configuration_digest
            ),
            lineage=lineage_for(fork_request, admission_ref=fork_admission.admission_ref),
            recorded_at=NOW,
        )
        assert await fork_repo.record(fork_request, fork_receipt) == fork_receipt
        assert await fork_repo.get(tenant_1, "fork-1") == fork_receipt
        concurrent_fork = fork_request.model_copy(
            update={
                "request_id": "fork-concurrent",
                "idempotency_key": "fork-concurrent",
                "derived_run_id": "run-fork-2",
            }
        )
        assert await fork_repo.reserve(concurrent_fork) is True
        admission_claims = await asyncio.gather(
            *(fork_repo.claim_admission(concurrent_fork) for _ in range(8))
        )
        assert sum(admission_claims) == 1
        recovery = await owner_rows(
            db,
            """
            SELECT f.request_key, r.kind, r.state FROM mission_control.fork_request f
            JOIN mission_control.recovery_request r
              USING (installation_id, application_id, tenant_id, recovery_id)
            ORDER BY f.request_key
            """,
        )
        assert [tuple(row.values()) for row in recovery] == [
            ("fork-1", "fork", "completed"),
            ("fork-concurrent", "fork", "admitted"),
        ]
        await _create_binding(
            pool,
            request_scope=tenant_2,
            run_id=tenant_two_run,
            binding_id="binding-2",
        )

        root = _lineage(
            lineage_id="lineage-1",
            run_id=run_id,
            runtime_attempt_id="runtime-1",
            request_scope=tenant_1,
        )
        child = _lineage(
            lineage_id="lineage-2",
            run_id=run_id,
            runtime_attempt_id="runtime-2",
            request_scope=tenant_1,
            parent_lineage_id=root.lineage_id,
            result_manifest_ref="result:accepted",
            recorded_at=NOW + timedelta(seconds=1),
        )

        async def append(lineage: PersistedExecutionLineage) -> PersistedExecutionLineage:
            async with pool.acquire() as connection, connection.transaction():
                await apply_scope(connection, lineage.envelope.request_scope)
                return await append_lineage_in_transaction(connection, lineage)

        assert await append(root) == await append(root)
        with pytest.raises(IdempotencyConflict, match="conflicting facts"):
            await append(
                _lineage(
                    lineage_id="lineage-1",
                    run_id=run_id,
                    runtime_attempt_id="runtime-other",
                    request_scope=tenant_1,
                )
            )
        await append(child)
        with pytest.raises(ValueError, match="parent must be persisted"):
            await append(
                _lineage(
                    lineage_id="lineage-orphan",
                    run_id=run_id,
                    runtime_attempt_id="runtime-orphan",
                    request_scope=tenant_1,
                    parent_lineage_id="lineage-missing",
                )
            )

        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, tenant_1)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.execution_lineage_record"
                )
                == 2
            )
            assert (
                await connection.fetchval(
                    """
                    SELECT count(*) FROM mission_control.execution_lineage_edge
                    WHERE relationship = 'attempt_of'
                    """
                )
                == 2
            )
            await apply_scope(connection, tenant_2)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.execution_lineage_record"
                )
                == 0
            )

        decision_repo = PostgresDecisionRepository(pool)
        decision = _decision_request(binding_id=binding.binding_id, request_scope=tenant_1)
        assert await decision_repo.create(decision) == await decision_repo.create(decision)
        with pytest.raises(IdempotencyConflict, match="conflicting intent"):
            conflicting = decision.model_copy(
                update={"policy_ref": "policy:other:1", "request_digest": DIGEST_B}
            )
            await decision_repo.create(conflicting)
        answered = await decision_repo.answer(decision, _decision_response(request_scope=tenant_1))
        assert answered.status == "answered"
        assert (
            await decision_repo.answer(decision, _decision_response(request_scope=tenant_1))
            == answered
        )
        with pytest.raises(IdempotencyConflict, match="different response"):
            await decision_repo.answer(
                decision,
                _decision_response(request_scope=tenant_1, response_digest=DIGEST_B),
            )
        loaded = await decision_repo.get(tenant_1, decision.decision_id)
        assert loaded is not None
        assert loaded.response is not None
        assert loaded.response.response_digest == DIGEST_A
        tasks = await owner_rows(
            db,
            """
            SELECT t.lifecycle, count(r.resolution_id) AS resolutions
            FROM mission_control.human_task t
            LEFT JOIN mission_control.human_resolution r
              USING (installation_id, application_id, tenant_id, human_task_id)
            WHERE t.task_key = 'runtime_decision:decision-1'
            GROUP BY t.lifecycle
            """,
        )
        assert [tuple(row.values()) for row in tasks] == [("resolved", 1)]

        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, tenant_2)
            scoped_counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM mission_control.runtime_execution_binding
                     WHERE binding_key = 'binding-1') AS bindings,
                    (SELECT count(*) FROM mission_control.human_task) AS decisions,
                    (SELECT count(*) FROM mission_control.fork_request
                     WHERE request_key = 'fork-1') AS forks,
                    (SELECT count(*) FROM mission_control.outbox
                     WHERE aggregate_key = $1) AS events
                """,
                run_id,
            )
            assert scoped_counts is not None
            assert all(value == 0 for value in scoped_counts.values())
        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, tenant_1)
            for statement in (
                "DELETE FROM mission_control.execution_lineage_record",
                "DELETE FROM mission_control.human_task",
                "UPDATE mission_control.human_resolution SET actor_ref = 'x'",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)
    finally:
        await pool.close()
