from __future__ import annotations

import asyncio

import asyncpg
import pytest

from mission_control.adapters.postgres.orchestration.linked_run_repository import (
    PostgresLinkedRunRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.domain.composition.contracts import (
    DependencyAssessment,
    LinkedRunResultAdmissionDecision,
    ResultEvidenceAssessment,
    RunCompositionLink,
    RunDependencyClass,
    RunDependencyRevision,
)
from mission_control.domain.policies.contracts import (
    CancelAction,
    ClaimEffectAction,
    CommandStatus,
    DecisionStatus,
)
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    create_common_database,
    drop_common_database,
    legacy_poison_intact,
)
from tests.integration.postgres.runtime_common import (
    assert_admission_lineage,
    owner_rows,
    scoped_command,
    scoped_count,
    scoped_request,
)
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.unit.run_control.test_run_control import service

pytestmark = pytest.mark.common_db


@pytest.mark.asyncio
async def test_postgres_atomic_rollback_and_concurrent_version_conflict(
    common_db: CommonDatabase,
) -> None:
    db = common_db
    tenant_1, tenant_2 = db.scope("tenant-1"), db.scope("tenant-2")

    def request(*, request_scope: str = "tenant-1", **kwargs: object):  # type: ignore[no-untyped-def]
        return scoped_request(db, request_scope, **kwargs)  # type: ignore[arg-type]

    def command(run_id: str, version: int, command_id: str, action: object):  # type: ignore[no-untyped-def]
        return scoped_command(db, run_id, version, command_id, action)

    pool = await db.pool("mission_control_runtime")
    try:

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
        assert len(await run_service.pending_outbox(tenant_1)) == 2
        replayed_admission = await run_service.admit(request())
        assert replayed_admission == admitted
        assert len(await run_service.pending_outbox(tenant_1)) == 2
        await assert_admission_lineage(db, admitted.run_id)
        # The injected failure rolled back every canonical admission row.
        assert len(await owner_rows(db, "SELECT 1 FROM mission_control.mission")) == 1
        first_page = await run_service.pending_outbox(tenant_1, limit=1)
        second_page = await run_service.pending_outbox(
            tenant_1, after=first_page[0].cursor, limit=1
        )
        assert len(second_page) == 1
        assert second_page[0].envelope.event_id != first_page[0].envelope.event_id

        tenant_two = await run_service.admit(
            request(request_scope="tenant-2", request_id="tenant-two")
        )
        assert tenant_two.status == DecisionStatus.ACCEPTED
        assert await scoped_count(pool, tenant_1, "mission_run") == 1
        assert await scoped_count(pool, tenant_2, "mission_run") == 1
        # Missing scope context returns no rows and denies writes.
        assert await scoped_count(pool, None, "mission_run") == 0
        async with pool.acquire() as connection, connection.transaction():
            with pytest.raises(asyncpg.PostgresError):
                await connection.execute(
                    "INSERT INTO mission_control.consumer_cursor (installation_id,"
                    " application_id, tenant_id, consumer_cursor_id, consumer_key,"
                    " aggregate_key, cursor, updated_at, created_at, created_by_actor_ref)"
                    " VALUES ($1, $2, $3, gen_random_uuid(), 'denied', 'denied', '{}'::jsonb,"
                    " now(), now(), 'x')",
                    db.installation_id,
                    db.application_id,
                    db.tenants["tenant-1"],
                )

        parent_budget = await run_service.get_budget(tenant_1, admitted.run_id)
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
        child_budget = await run_service.get_budget(tenant_1, first_child_run_id)
        composition = PostgresLinkedRunRepository(pool)
        link = RunCompositionLink(
            link_id="postgres-link-1",
            request_identity="postgres-linked-request-1",
            request_fingerprint="sha256:" + "a" * 64,
            request_scope=tenant_1,
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
        assert await composition.commit_dependency_revision(tenant_1, revision) == revision
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
            await composition.commit_result_decision(tenant_1, result_decision) == result_decision
        )
        assert await composition.list_parent_links(tenant_1, admitted.run_id) == (link,)
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

        async def fail_effect_command(boundary: str) -> None:
            if boundary == "command":
                raise RuntimeError("injected effect transaction failure")

        failing_command_service, _ = service(
            PostgresRunControlRepository(pool, before_commit=fail_effect_command)  # type: ignore[arg-type]
        )
        with pytest.raises(RuntimeError, match="effect transaction failure"):
            await failing_command_service.execute(
                command(
                    admitted.run_id,
                    2,
                    "claim-effect-rollback",
                    ClaimEffectAction(
                        effect_id="effect:rollback",
                        effect_kind="external.test",
                        operation_ref="operation:rollback",
                        provider_idempotency_key="provider-key:rollback",
                        reservation_id="baseline",
                    ),
                )
            )
        assert (await run_service.get_run(tenant_1, admitted.run_id)).version == 2
        assert (await run_service.get_effects(tenant_1, admitted.run_id)).claims == {}
        assert await run_service.list_effect_ledger(tenant_1, admitted.run_id) == ()

        first, second = await asyncio.gather(
            run_service.execute(command(admitted.run_id, 2, "cancel-a", CancelAction())),
            run_service.execute(command(admitted.run_id, 2, "cancel-b", CancelAction())),
        )
        assert {first.status, second.status} == {
            CommandStatus.ACCEPTED,
            CommandStatus.STALE,
        }
        assert (await run_service.get_run(tenant_1, admitted.run_id)).version == 3
        assert await run_service.reconstruct_projection(
            tenant_1, admitted.run_id
        ) == await run_service.get_run(tenant_1, admitted.run_id)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_equal_tenant_uuids_in_two_installations_cannot_cross() -> None:
    biotech = await create_common_database(application_id="biotech")
    other = await create_common_database(application_id="ai-engineer")
    try:
        assert biotech.tenants["tenant-1"] == other.tenants["tenant-1"]
        biotech_pool = await biotech.pool()
        other_pool = await other.pool()
        try:
            service_a, _ = service(PostgresRunControlRepository(biotech_pool))  # type: ignore[arg-type]
            admitted = await service_a.admit(scoped_request(biotech))
            assert admitted.run_id is not None
            # Same tenant UUID, other installation: no rows in either direction.
            assert await scoped_count(other_pool, other.scope("tenant-1"), "mission_run") == 0
            assert await scoped_count(biotech_pool, other.scope("tenant-1"), "mission_run") == 0
            repository = PostgresRunControlRepository(biotech_pool)
            with pytest.raises(Exception):  # noqa: B017 - denied by scope, never a fallback
                await repository.get_run(other.scope("tenant-1"), admitted.run_id)
            with pytest.raises(ValueError):
                await repository.get_run("tenant-1", admitted.run_id)
        finally:
            await biotech_pool.close()
            await other_pool.close()
    finally:
        await drop_common_database(biotech)
        await drop_common_database(other)


@pytest.mark.asyncio
async def test_admission_with_legacy_poison_never_reads_legacy() -> None:
    db = await create_common_database(legacy_poison=True)
    try:
        pool = await db.pool()
        try:
            run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
            admitted = await run_service.admit(scoped_request(db))
            assert admitted.status == DecisionStatus.ACCEPTED
            assert admitted.run_id is not None
            await run_service.execute(
                scoped_command(db, admitted.run_id, 1, "start", {"kind": "start"})
            )
            await assert_admission_lineage(db, admitted.run_id)
            async with pool.acquire() as connection:
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.fetch("SELECT * FROM belllabs_control.workflow_runs")
        finally:
            await pool.close()
        assert await legacy_poison_intact(db.owner_dsn)
    finally:
        await drop_common_database(db)


@pytest.mark.asyncio
async def test_outbox_lease_delivery_consumer_cursor_and_idempotency_conflict(
    common_db: CommonDatabase,
) -> None:
    from datetime import timedelta

    from mission_control.domain.policies.contracts import ConsumerApplyStatus
    from mission_control.domain.policies.errors import IdempotencyConflict

    db = common_db
    scope = db.scope()
    pool = await db.pool(max_size=6)
    try:
        repository = PostgresRunControlRepository(pool)
        run_service, _ = service(repository)  # type: ignore[arg-type]
        admitted = await run_service.admit(scoped_request(db, request_id="outbox-lease"))
        assert admitted.run_id is not None
        # A changed request under the same identity conflicts; nothing new is written.
        with pytest.raises(IdempotencyConflict):
            await run_service.admit(scoped_request(db, request_id="outbox-lease", hard_cap=90))
        assert len(await owner_rows(db, "SELECT 1 FROM mission_control.request_receipt")) == 1

        pending = await run_service.pending_outbox(scope)
        assert [item.envelope.event_type for item in pending] == [
            "workflow_run.admitted",
            "workflow_run.start_requested",
        ]
        now = pending[0].envelope.recorded_at
        # Two relays claim disjoint rows with SKIP LOCKED leases.
        first, second = await asyncio.gather(
            repository.lease_outbox(
                scope,
                lease_owner="relay-a",
                lease_until=now + timedelta(minutes=1),
                now=now,
                limit=1,
            ),
            repository.lease_outbox(
                scope,
                lease_owner="relay-b",
                lease_until=now + timedelta(minutes=1),
                now=now,
                limit=1,
            ),
        )
        leased = {item.envelope.event_id for item in (*first, *second)}
        assert len(leased) == 2
        # A live lease is not re-leased; an expired one is.
        assert (
            await repository.lease_outbox(
                scope, lease_owner="relay-c", lease_until=now + timedelta(minutes=2), now=now
            )
            == ()
        )
        expired = await repository.lease_outbox(
            scope,
            lease_owner="relay-c",
            lease_until=now + timedelta(minutes=5),
            now=now + timedelta(minutes=2),
        )
        assert {item.envelope.event_id for item in expired} == leased
        for item in pending:
            await run_service.mark_delivered(scope, item.envelope.event_id, now)
        assert await run_service.pending_outbox(scope) == ()
        states = await owner_rows(
            db, "SELECT DISTINCT delivery_state, lease_owner FROM mission_control.outbox"
        )
        assert [tuple(row.values()) for row in states] == [("delivered", None)]

        # The consumer cursor applies each authoritative event once, in order.
        first_event, second_event = pending[0].envelope, pending[1].envelope
        applied = await run_service.apply_consumer_event(scope, "projector", first_event)
        assert applied.status == ConsumerApplyStatus.APPLIED
        duplicate = await run_service.apply_consumer_event(scope, "projector", first_event)
        assert duplicate.status == ConsumerApplyStatus.DUPLICATE
        assert (
            await run_service.apply_consumer_event(scope, "projector", second_event)
        ).status == ConsumerApplyStatus.APPLIED
    finally:
        await pool.close()
