from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import asyncpg
import pytest

from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.application.postgres_runtime_authority import (
    PostgresBootstrapAuthority,
    PostgresBootstrapDecisionBridge,
)
from app.application.postgres_runtime_execution_repository import (
    PostgresRuntimeCoordinationRepository,
)
from app.application.postgres_stage3_kernel_repository import (
    RETENTION_DAYS,
    PostgresDecisionRepository,
    PostgresExecutionLineageRepository,
    PostgresForkRepository,
    PostgresResourceLeaseJournal,
    PostgresRuntimeIncidentRepository,
    PostgresStage3RetentionRepository,
)
from app.application.runtime_bootstrap import BootstrapRequest
from app.application.runtime_lineage import PersistedExecutionLineage
from app.application.runtime_reconciliation import (
    RuntimeIncidentDecision,
    RuntimeIncidentObservation,
    RuntimeIncidentType,
    RuntimeRepairAuditRecord,
)
from app.application.runtime_recovery import ForkAdmission
from app.application.runtime_resources import ResourceCapacity, ResourceExhausted
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.contracts import (
    ActorRef,
    Correlation,
    ForkReceipt,
    ForkRequest,
    GraphExecutionSubmission,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.definitions import (
    ContentAddressedRef,
    ExecutionLineageEnvelope,
    RuntimeDefinitionKind,
)
from app.domain.graph_runtime.identities import (
    AgentThreadKey,
    BellLabsRunKey,
    ExecutionEpochKey,
    LangGraphCheckpointKey,
)
from app.domain.graph_runtime.kernel import (
    DecisionRequest,
    DecisionResponse,
    LineageKind,
    LineageParentEdge,
    ProviderQualifiedLineageRecord,
    ResourceKind,
    ResourceLeaseRequest,
    ResourceLeaseStatus,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.postgres import apply_application_migrations
from tests.test_run_control import request as run_request
from tests.test_run_control import service as run_control_service

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


class AllowRetention:
    async def authorize_deletion(
        self,
        *,
        request_scope: str,
        actor_id: str,
        record_class: str,
    ) -> bool:
        return (
            request_scope == "tenant-1"
            and actor_id == "operator:retention"
            and record_class
            in {"checkpoint", "event", "incident", "lineage", "decision", "fork"}
        )


def _require_disposable_postgres(dsn: str) -> None:
    parsed = urlparse(dsn)
    if (
        parsed.hostname not in {"127.0.0.1", "localhost"}
        or parsed.port != 55432
        or parsed.path != "/belllabs"
        or parsed.username != "belllabs"
    ):
        raise RuntimeError(
            "Stage 3 kernel postgres proof requires the disposable local PostgreSQL target"
        )


async def _reset_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
    await apply_application_migrations(pool)


async def _admit_run(pool: asyncpg.Pool, *, request_scope: str = "tenant-1") -> str:
    run_service, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    decision = await run_service.admit(run_request(request_scope=request_scope))
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
    request_scope: str = "tenant-1",
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


def _lease_request(
    lease_id: str,
    semantic_identity: str,
    resources: tuple[ResourceKind, ...],
    *,
    digest: str = DIGEST_A,
    ttl_seconds: int = 60,
    request_scope: str = "tenant-1",
) -> ResourceLeaseRequest:
    return ResourceLeaseRequest(
        lease_id=lease_id,
        request_scope=request_scope,
        semantic_identity=semantic_identity,
        envelope_digest=digest,
        resources=resources,
        requested_at=NOW,
        deadline=NOW + timedelta(minutes=10),
        ttl_seconds=ttl_seconds,
    )


def _decision_request(
    *,
    binding_id: str,
    request_scope: str = "tenant-1",
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
    request_scope: str = "tenant-1",
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


def _incident(
    *,
    incident_id: str = "incident-1",
    request_scope: str = "tenant-1",
    identity_digest: str = DIGEST_A,
) -> RuntimeIncidentObservation:
    return RuntimeIncidentObservation(
        incident_id=incident_id,
        request_scope=request_scope,
        binding_id=None,
        incident_type=RuntimeIncidentType.EXPIRED_RESOURCE_LEASE,
        identity_digest=identity_digest,
        observed_version=3,
        expected_version=3,
        evidence_refs=("evidence:1",),
        proposed_action="expire_lease",
        observed_at=NOW,
    )


@pytest.mark.asyncio
async def test_stage3_kernel_postgres_persistence_slice(
    test_application_postgres_dsn: str,
) -> None:
    _require_disposable_postgres(test_application_postgres_dsn)
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=6)
    try:
        await _reset_schema(pool)
        await apply_application_migrations(pool)
        async with pool.acquire() as connection:
            versions = {
                row["version"]
                for row in await connection.fetch(
                    "SELECT version FROM belllabs_control.schema_migrations"
                )
            }
            assert "0014_stage3_durable_runtime_kernel.sql" in versions
            status_check = await connection.fetchval(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = 'belllabs_control.execution_resource_leases'::regclass
                  AND contype = 'c'
                  AND pg_get_constraintdef(oid) LIKE '%reconciliation_required%'
                LIMIT 1
                """
            )
            assert status_check is not None
            assert "acquired" in status_check
            assert "requested" in status_check
            assert "reconciliation_required" in status_check
            assert "reserved" not in status_check

        run_id = await _admit_run(pool, request_scope="tenant-1")
        tenant_two_run = await _admit_run(pool, request_scope="tenant-2")
        binding = await _create_binding(
            pool,
            request_scope="tenant-1",
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
        bootstrap_decision_id = (
            await PostgresBootstrapDecisionBridge(
                pool
            ).persist_reconciliation_decision(
                bootstrap_request,
                bootstrap_projection,
                "checkpoint_route_incompatible",
            )
        )
        assert bootstrap_decision_id.startswith("decision-")
        fork_request = ForkRequest(
            request_id="fork-1",
            idempotency_key="fork-1",
            source_epoch=binding.epoch,
            source_checkpoint=LangGraphCheckpointKey(
                deployment_endpoint_id="endpoint-1",
                agent_server_thread_id="thread-1",
                langgraph_checkpoint_id="checkpoint-1",
            ),
            target_run=BellLabsRunKey(
                request_scope="tenant-1",
                belllabs_run_id="run-fork-1",
            ),
            run_plan_digest=binding.run_plan_digest,
            actor=ActorRef(
                actor_id="operator-1",
                actor_type="operator",
                authority_ref="authority:operator@1",
            ),
            reason="durable recovery fork",
            requested_at=NOW,
        )
        fork_repo = PostgresForkRepository(pool)
        assert await fork_repo.reserve(fork_request) is True
        assert await fork_repo.reserve(fork_request) is False
        assert await fork_repo.claim_admission(fork_request) is True
        assert await fork_repo.claim_admission(fork_request) is False
        fork_admission = ForkAdmission(
            request_id="fork-1",
            target_epoch=ExecutionEpochKey(
                request_scope="tenant-1",
                belllabs_run_id="run-fork-1",
                execution_epoch=1,
            ),
            budget_reservation_ref="budget:fork-1",
            admitted_run_plan_digest=binding.run_plan_digest,
        )
        assert (
            await fork_repo.record_admission(fork_request, fork_admission)
            == fork_admission
        )
        assert await fork_repo.claim_copy(fork_request) is True
        assert await fork_repo.claim_copy(fork_request) is False
        fork_receipt = ForkReceipt(
            request_id="fork-1",
            source_epoch=binding.epoch,
            target_epoch=fork_admission.target_epoch,
            target_thread=AgentThreadKey(
                **fork_admission.target_epoch.model_dump(),
                agent_server_thread_id="thread-fork-1",
                relationship="fork",
                parent_belllabs_run_id=run_id,
            ),
            status="accepted",
            recorded_at=NOW,
        )
        assert await fork_repo.record(fork_request, fork_receipt) == fork_receipt
        assert await fork_repo.get("tenant-1", "fork-1") == fork_receipt
        concurrent_fork = fork_request.model_copy(
            update={"request_id": "fork-concurrent", "idempotency_key": "fork-concurrent"}
        )
        assert await fork_repo.reserve(concurrent_fork) is True
        admission_claims = await asyncio.gather(
            *(fork_repo.claim_admission(concurrent_fork) for _ in range(8))
        )
        assert sum(admission_claims) == 1
        await _create_binding(
            pool,
            request_scope="tenant-2",
            run_id=tenant_two_run,
            binding_id="binding-2",
        )
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_checkpoint_observations (
                    observation_id, request_scope, binding_id, deployment_endpoint_id,
                    agent_server_thread_id, langgraph_checkpoint_id, state_schema_digest,
                    graph_assembly_digest, status, summary_digest, redacted_summary,
                    observed_at
                )
                VALUES (
                    'checkpoint-observation-1', 'tenant-1', 'binding-1', 'endpoint-1',
                    'thread-1', 'checkpoint-1', $1, $1, 'observed', $1, '{}'::jsonb, $2
                )
                """,
                DIGEST_A,
                NOW,
            )

        lineage_repo = PostgresExecutionLineageRepository(pool)
        root = _lineage(lineage_id="lineage-1", run_id=run_id, runtime_attempt_id="runtime-1")
        child = _lineage(
            lineage_id="lineage-2",
            run_id=run_id,
            runtime_attempt_id="runtime-2",
            parent_lineage_id=root.lineage_id,
            result_manifest_ref="result:accepted",
            recorded_at=NOW + timedelta(seconds=1),
        )
        assert await lineage_repo.append(root) == await lineage_repo.append(root)
        with pytest.raises(IdempotencyConflict, match="conflicting facts"):
            await lineage_repo.append(
                _lineage(
                    lineage_id="lineage-1",
                    run_id=run_id,
                    runtime_attempt_id="runtime-other",
                )
            )
        await lineage_repo.append(child)
        provenance = await lineage_repo.provenance_for_result("tenant-1", "result:accepted")
        assert [item.lineage_id for item in provenance] == ["lineage-1", "lineage-2"]

        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.runtime_lineage_records"
                )
                == 2
            )
            assert (
                await connection.fetchval(
                    """
                    SELECT count(*)
                    FROM belllabs_control.runtime_lineage_edges
                    WHERE relationship = 'attempt_of'
                    """
                )
                == 2
            )
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.runtime_lineage_records"
                )
                == 0
            )

        decision_repo = PostgresDecisionRepository(pool)
        decision = _decision_request(binding_id=binding.binding_id)
        assert await decision_repo.create(decision) == await decision_repo.create(decision)
        with pytest.raises(IdempotencyConflict, match="conflicting intent"):
            conflicting = decision.model_copy(
                update={"policy_ref": "policy:other:1", "request_digest": DIGEST_B}
            )
            await decision_repo.create(conflicting)
        answered = await decision_repo.answer(decision, _decision_response())
        assert answered.status == "answered"
        assert await decision_repo.answer(decision, _decision_response()) == answered
        with pytest.raises(IdempotencyConflict, match="different response"):
            await decision_repo.answer(
                decision,
                _decision_response(response_digest=DIGEST_B),
            )
        loaded = await decision_repo.get("tenant-1", decision.decision_id)
        assert loaded is not None
        assert loaded.response is not None
        assert loaded.response.response_digest == DIGEST_A

        leases = PostgresResourceLeaseJournal(
            pool,
            ResourceCapacity(
                limits={
                    ResourceKind.TENANT: 4,
                    ResourceKind.OPERATION_WORKER: 1,
                    ResourceKind.RESUMPTION: 1,
                    ResourceKind.MODEL_CALL: 1,
                }
            ),
        )
        worker = await leases.acquire(
            _lease_request(
                "worker-1",
                "operation:1",
                (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
            ),
            now=NOW,
        )
        assert await leases.acquire(
            _lease_request(
                "worker-1",
                "operation:1",
                (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
            ),
            now=NOW,
        ) == worker
        with pytest.raises(IdempotencyConflict, match="different envelope"):
            await leases.acquire(
                _lease_request(
                    "worker-2",
                    "operation:1",
                    (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
                    digest=DIGEST_B,
                ),
                now=NOW,
            )
        with pytest.raises(ResourceExhausted, match="operation_worker"):
            await leases.acquire(
                _lease_request(
                    "worker-2",
                    "operation:2",
                    (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
                ),
                now=NOW,
            )
        resumed = await leases.acquire(
            _lease_request(
                "resume-1",
                "resume:operation:waiting",
                (ResourceKind.TENANT, ResourceKind.RESUMPTION),
            ),
            now=NOW,
        )
        assert resumed.status == ResourceLeaseStatus.ACQUIRED
        model = await leases.acquire(
            _lease_request(
                "model-1",
                "model:1",
                (ResourceKind.TENANT, ResourceKind.MODEL_CALL),
            ),
            now=NOW,
        )
        wait = await leases.transition_to_wait(
            request_scope="tenant-1",
            wait_binding_ref="wait:1",
            lease_ids=(worker.request.lease_id, model.request.lease_id),
            retain=frozenset({model.request.lease_id}),
            now=NOW,
        )
        assert wait.released_reservations == (worker.request.lease_id,)
        assert wait.retained_reservations == (model.request.lease_id,)
        replacement = await leases.acquire(
            _lease_request(
                "worker-3",
                "operation:3",
                (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
                ttl_seconds=1,
            ),
            now=NOW,
        )
        expired = await leases.expire_due(
            request_scope="tenant-1",
            now=NOW + timedelta(seconds=2),
        )
        assert any(item.request.lease_id == replacement.request.lease_id for item in expired)
        reacquired = await leases.acquire(
            _lease_request(
                "worker-4",
                "operation:4",
                (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
            ),
            now=NOW + timedelta(seconds=2),
        )
        assert reacquired.status == ResourceLeaseStatus.ACQUIRED
        released = await leases.release(
            request_scope="tenant-1",
            lease_id=model.request.lease_id,
            expected_digest=model.canonical_digest,
            now=NOW + timedelta(seconds=2),
        )
        assert released.status == ResourceLeaseStatus.RELEASED

        incidents = PostgresRuntimeIncidentRepository(pool)
        observation = _incident()
        assert await incidents.reserve_incident(observation) is True
        assert await incidents.reserve_incident(observation) is False
        decision_record = RuntimeIncidentDecision(
            incident_id=observation.incident_id,
            request_scope=observation.request_scope,
            action="expire_lease",
            disposition="automatic",
            before_version=3,
            after_version=4,
            actor_ref="service:runtime-reconciler",
            reason="idempotent version-checked automatic repair",
            evidence_refs=observation.evidence_refs,
        )
        assert (
            await incidents.record_incident_decision(observation, decision_record)
            == decision_record
        )
        assert (
            await incidents.record_incident_decision(observation, decision_record)
            == decision_record
        )
        with pytest.raises(ValueError, match="changed its decision"):
            await incidents.record_incident_decision(
                observation,
                decision_record.model_copy(update={"reason": "different"}),
            )
        repair_audit = RuntimeRepairAuditRecord(
            request_scope="tenant-1",
            audit_id="repair-audit-1",
            incident_id=observation.incident_id,
            command_id="repair-command-1",
            actor_id="operator-1",
            reason="approved version-checked repair",
            expected_belllabs_version=3,
            expected_checkpoint_id="checkpoint-1",
            before_digest=DIGEST_A,
            after_digest=DIGEST_B,
            evidence_refs=("evidence:repair",),
            recorded_at=NOW,
        )
        assert await incidents.record_repair_audit(repair_audit) == repair_audit
        assert await incidents.record_repair_audit(repair_audit) == repair_audit
        with pytest.raises(IdempotencyConflict, match="conflicting facts"):
            await incidents.record_repair_audit(
                repair_audit.model_copy(update={"reason": "changed repair reason"})
            )
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            scoped_counts = await connection.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM belllabs_control.runtime_execution_bindings
                     WHERE binding_id = 'binding-1') AS bindings,
                    (SELECT count(*) FROM belllabs_control.runtime_checkpoint_observations
                     WHERE observation_id = 'checkpoint-observation-1') AS checkpoints,
                    (SELECT count(*) FROM belllabs_control.execution_resource_leases
                     WHERE lease_id = 'worker-4') AS leases,
                    (SELECT count(*) FROM belllabs_control.runtime_decision_requests
                     WHERE decision_id = 'decision-1') AS decisions,
                    (SELECT count(*) FROM belllabs_control.runtime_reconciliation_incidents
                     WHERE incident_id = 'incident-1') AS incidents,
                    (SELECT count(*) FROM belllabs_control.runtime_repair_audit
                     WHERE audit_id = 'repair-audit-1') AS repairs,
                    (SELECT count(*) FROM belllabs_control.runtime_fork_requests
                     WHERE request_id = 'fork-1') AS forks,
                    (SELECT count(*) FROM belllabs_control.outbox
                     WHERE aggregate_id = $1) AS events
                """,
                run_id,
            )
            assert scoped_counts is not None
            assert all(value == 0 for value in scoped_counts.values())

        with pytest.raises(PermissionError, match="lacks scoped operator"):
            await PostgresStage3RetentionRepository(pool).delete_expired(
                request_scope="tenant-1",
                record_class="lineage",
                cutoff_at=NOW,
                actor_id="operator:retention",
                reason="unauthorized purge",
                deletion_id="deletion-denied",
                recorded_at=NOW,
            )
        retention = PostgresStage3RetentionRepository(pool, AllowRetention())
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE belllabs_control.outbox
                SET delivered_at = $2
                WHERE aggregate_id = $1
                """,
                run_id,
                NOW,
            )
        stale_template = _lineage(
            lineage_id="lineage-stale",
            run_id=run_id,
            runtime_attempt_id="runtime-stale",
        )
        stale = PersistedExecutionLineage.create(
            lineage_id=stale_template.lineage_id,
            envelope=stale_template.envelope,
            qualified_identities=stale_template.qualified_identities,
            recorded_at=NOW - timedelta(days=120),
            retain_until=NOW - timedelta(days=1),
        )
        await lineage_repo.append(stale)
        deleted = await retention.delete_expired(
            request_scope="tenant-1",
            record_class="lineage",
            cutoff_at=NOW,
            actor_id="operator:retention",
            reason="90-day audited purge",
            deletion_id="deletion-lineage-1",
            recorded_at=NOW,
        )
        assert deleted >= 1
        assert (
            await retention.delete_expired(
                request_scope="tenant-1",
                record_class="lineage",
                cutoff_at=NOW,
                actor_id="operator:retention",
                reason="90-day audited purge",
                deletion_id="deletion-lineage-1",
                recorded_at=NOW,
            )
            == deleted
        )
        for record_class in ("checkpoint", "event", "incident", "decision", "fork"):
            assert (
                await retention.delete_expired(
                    request_scope="tenant-1",
                    record_class=record_class,
                    cutoff_at=NOW,
                    actor_id="operator:retention",
                    reason="90-day audited purge",
                    deletion_id=f"deletion-{record_class}-1",
                    recorded_at=NOW,
                )
                == 0
            )
        async with pool.acquire() as connection:
            recent_events = await connection.fetchval(
                """
                SELECT count(*) FROM belllabs_control.outbox
                WHERE aggregate_id = $1
                """,
                run_id,
            )
            assert recent_events > 0
            await connection.execute(
                """
                UPDATE belllabs_control.outbox
                SET recorded_at = $2
                WHERE aggregate_id = $1
                """,
                run_id,
                NOW - timedelta(days=91),
            )
        assert (
            await retention.delete_expired(
                request_scope="tenant-1",
                record_class="event",
                cutoff_at=NOW,
                actor_id="operator:retention",
                reason="90-day audited purge",
                deletion_id="deletion-event-expired",
                recorded_at=NOW,
            )
            == recent_events
        )
        with pytest.raises(LookupError, match="no persisted lineage"):
            await lineage_repo.provenance_for_result("tenant-2", "result:accepted")
    finally:
        await pool.close()
