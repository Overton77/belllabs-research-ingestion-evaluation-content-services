from __future__ import annotations

import asyncio

import asyncpg
import pytest

from app.application.postgres_linked_run_repository import PostgresLinkedRunRepository
from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.composition.contracts import (
    DependencyAssessment,
    LinkedRunResultAdmissionDecision,
    ResultEvidenceAssessment,
    RunCompositionLink,
    RunDependencyClass,
    RunDependencyRevision,
)
from app.domain.run_control.contracts import CancelAction, CommandStatus, DecisionStatus
from app.integrations.postgres import apply_application_migrations
from tests.test_run_control import command, request, service


@pytest.mark.asyncio
async def test_postgres_atomic_rollback_and_concurrent_version_conflict(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=6)
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)

        async def fail_admission(boundary: str) -> None:
            if boundary == "admission":
                raise RuntimeError("injected transaction failure")

        failing_repository = PostgresRunControlRepository(pool, before_commit=fail_admission)
        failing_service, _ = service(failing_repository)  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="injected transaction failure"):
            await failing_service.admit(request())

        repository = PostgresRunControlRepository(pool)
        run_service, _ = service(repository)  # type: ignore[arg-type]
        admitted = await run_service.admit(request())
        assert admitted.status == DecisionStatus.ACCEPTED
        assert admitted.run_id is not None
        assert len(await run_service.pending_outbox("tenant-1")) == 2
        first_page = await run_service.pending_outbox("tenant-1", limit=1)
        second_page = await run_service.pending_outbox(
            "tenant-1", after=first_page[0].cursor, limit=1
        )
        assert len(second_page) == 1
        assert second_page[0].envelope.event_id != first_page[0].envelope.event_id

        tenant_two = await run_service.admit(
            request(request_scope="tenant-2", request_id="tenant-two")
        )
        assert tenant_two.status == DecisionStatus.ACCEPTED
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            assert (
                await connection.fetchval("SELECT count(*) FROM belllabs_control.workflow_runs")
                == 1
            )
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval("SELECT count(*) FROM belllabs_control.workflow_runs")
                == 1
            )

        parent_budget = await run_service.get_budget("tenant-1", admitted.run_id)
        first_child_run_id: str | None = None
        for index in range(4):
            child_request = request(request_id=f"postgres-child-{index}")
            child_request = child_request.model_copy(
                update={
                    "parent_run_id": admitted.run_id,
                    "actor": child_request.actor.model_copy(
                        update={
                            "authority_refs": child_request.actor.authority_refs
                            | {f"workflow_run.parent:{admitted.run_id}:sponsor"}
                        }
                    ),
                    "budget_envelope": child_request.budget_envelope.model_copy(
                        update={"parent_account_id": parent_budget.account_id}
                    ),
                }
            )
            child_decision = await run_service.admit(child_request)
            assert child_decision.status == DecisionStatus.ACCEPTED
            first_child_run_id = first_child_run_id or child_decision.run_id
        assert first_child_run_id is not None
        child_budget = await run_service.get_budget("tenant-1", first_child_run_id)
        composition = PostgresLinkedRunRepository(pool)
        link = RunCompositionLink(
            link_id="postgres-link-1",
            request_identity="postgres-linked-request-1",
            request_fingerprint="sha256:" + "a" * 64,
            request_scope="tenant-1",
            parent_run_id=admitted.run_id,
            child_run_id=first_child_run_id,
            slot_id="child_work",
            request_revision=1,
            target_workflow_type_ref=request().workflow_type_ref,
            child_effective_configuration_digest=request().effective_configuration_digest,
            dependency_class=RunDependencyClass.REQUIRED_BLOCKING,
            linked_budget_account_id=child_budget.account_id,
            result_admission_policy="linked-result:exact@1",
            cancellation_policy="request_cancel",
            created_at=request().requested_at,
        )
        assert await composition.commit_link(link) == link
        assert await composition.commit_link(link) == link
        assert tenant_two.run_id is not None
        cross_tenant_link = link.model_copy(
            update={
                "link_id": "postgres-link-cross-tenant",
                "request_identity": "postgres-linked-request-cross-tenant",
                "child_run_id": tenant_two.run_id,
            }
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await composition.commit_link(cross_tenant_link)
        revision = RunDependencyRevision(
            revision_id="postgres-dependency-revision-2",
            link_id=link.link_id,
            revision=2,
            prior_dependency_class=RunDependencyClass.REQUIRED_BLOCKING,
            dependency_class=RunDependencyClass.DEGRADABLE_NONBLOCKING,
            assessment=DependencyAssessment(
                readiness_reassessment_required=True,
                reason="integration revision",
            ),
            authority_ref="authority:integration",
            decided_by="integration-test",
            decided_at=request().requested_at,
        )
        assert (
            await composition.commit_dependency_revision("tenant-1", revision)
            == revision
        )
        result_decision = LinkedRunResultAdmissionDecision(
            decision_id="postgres-linked-result-1",
            link_id=link.link_id,
            parent_run_id=link.parent_run_id,
            child_run_id=link.child_run_id,
            exact_output_ref="artifact:child:exact-v1",
            outcome="admit",
            assessment=ResultEvidenceAssessment(
                intended_purpose_satisfied=True,
                exact_version_compatible=True,
                ready=True,
                provenance_valid=True,
                permissions_valid=True,
                evaluation_evidence_valid=True,
            ),
            authority_ref="authority:integration",
            decided_by="integration-test",
            decided_at=request().requested_at,
            reason="integration result admission",
        )
        assert (
            await composition.commit_result_decision("tenant-1", result_decision)
            == result_decision
        )
        assert await composition.list_parent_links("tenant-1", admitted.run_id) == (
            link,
        )
        over_cap = request(request_id="postgres-child-over-cap")
        over_cap = over_cap.model_copy(
            update={
                "parent_run_id": admitted.run_id,
                "actor": over_cap.actor.model_copy(
                    update={
                        "authority_refs": over_cap.actor.authority_refs
                        | {f"workflow_run.parent:{admitted.run_id}:sponsor"}
                    }
                ),
                "budget_envelope": over_cap.budget_envelope.model_copy(
                    update={"parent_account_id": parent_budget.account_id}
                ),
            }
        )
        assert (await run_service.admit(over_cap)).status == DecisionStatus.REJECTED

        await run_service.execute(command(admitted.run_id, 1, "start", {"kind": "start"}))
        first, second = await asyncio.gather(
            run_service.execute(command(admitted.run_id, 2, "cancel-a", CancelAction())),
            run_service.execute(command(admitted.run_id, 2, "cancel-b", CancelAction())),
        )
        assert {first.status, second.status} == {
            CommandStatus.ACCEPTED,
            CommandStatus.STALE,
        }
        assert (await run_service.get_run("tenant-1", admitted.run_id)).version == 3
        assert await run_service.reconstruct_projection(
            "tenant-1", admitted.run_id
        ) == await run_service.get_run("tenant-1", admitted.run_id)
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await pool.close()
