"""Real SQL authority behind the scoped HTTP facade; no provider/model execution.

Uses additive unique identities and never drops schemas or existing rows. Configure an
isolated test database via TEST_APPLICATION_POSTGRES_DSN; migrations require its owner.
"""

from uuid import uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from mission_control.adapters.postgres.connections import apply_application_migrations
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.application.execution.boundary_interventions import (
    BoundaryCommandApplicationService,
    BoundaryCommandDeliveryService,
    BoundaryDeliveryResult,
    BoundaryInterventionService,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
    VerifiedApplicationIdentity,
    request_scope,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.domain.policies.contracts import (
    ApplyBoundaryCommandAction,
    BoundaryCommandStatus,
    StartAction,
)
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    router,
)
from tests.unit.run_control.test_boundary_commands import TARGET, pause
from tests.unit.run_control.test_mission_control_facade import pause_request
from tests.unit.run_control.test_run_control import actor, command, request, service


class AcknowledgedBoundary:
    """Deterministic delivery acknowledgement; application is independently recorded below."""

    async def deliver(self, status: BoundaryCommandStatus) -> BoundaryDeliveryResult:
        return BoundaryDeliveryResult("delivered", status.command.target.target_ref or "boundary")


@pytest.mark.asyncio
async def test_scoped_http_pause_replay_apply_and_database_rls(
    test_application_postgres_dsn: str,
) -> None:
    migration_pool = await asyncpg.create_pool(
        test_application_postgres_dsn, min_size=1, max_size=1
    )
    try:
        await apply_application_migrations(migration_pool)
    finally:
        await migration_pool.close()

    async def assume_runtime(connection: asyncpg.Connection) -> None:
        await connection.execute("SET ROLE belllabs_control_runtime")

    pool = await asyncpg.create_pool(
        test_application_postgres_dsn, min_size=1, max_size=3, setup=assume_runtime
    )
    try:
        installation, tenant = uuid4(), uuid4()
        identity = VerifiedApplicationIdentity(
            issuer="https://parity.invalid/auth/v1",
            audiences=frozenset({"authenticated"}),
            application_id="biotech",
            installation_id=installation,
            tenant_id=tenant,
        )
        scope = request_scope(identity)
        repository = PostgresRunControlRepository(pool)
        authority, _ = service(repository)  # type: ignore[arg-type]
        admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
        assert admission.run_id is not None
        run_id = admission.run_id
        await authority.execute(
            command(run_id, 1, "start", StartAction(execution_target=TARGET)).model_copy(
                update={"request_scope": scope}
            )
        )
        facade = MissionControlService(
            authority,
            BoundaryInterventionService(
                authority, BoundaryCommandDeliveryService(authority, AcknowledgedBoundary())
            ),
            request_scope=scope,
        )
        registry = ApplicationRegistry(
            (
                ApplicationBinding.seal(
                    application_id="biotech",
                    installation_id=installation,
                    binding_version="1",
                    supabase_project_ref="local-parity",
                    database_secret_ref="TEST_APPLICATION_POSTGRES_DSN",
                    accepted_issuers={identity.issuer},
                    accepted_audiences=identity.audiences,
                    required_component_version="parity-1",
                ),
            )
        )
        registry.observe(
            "biotech",
            InstallationObservation(
                installation, "biotech", "local-parity", frozenset({"parity-1"})
            ),
        )
        trusted_actor = actor().model_copy(
            update={"permissions": actor().permissions | {"workflow_run.read"}}
        )
        principal = MissionPrincipal(
            installation_id=installation,
            application_id="biotech",
            tenant_id=tenant,
            issuer=identity.issuer,
            audiences=identity.audiences,
            actor=trusted_actor,
        )
        app = FastAPI()
        app.include_router(router)
        app.state.mission_control_registry = registry
        app.state.mission_control_services = {(installation, "biotech", tenant): facade}
        app.dependency_overrides[get_mission_principal] = lambda: principal
        base = f"/v1/applications/biotech/runs/{run_id}"
        intent = pause_request(run_id)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://local-test"
        ) as client:
            result = await client.post(f"{base}/commands", json=intent.model_dump(mode="json"))
            assert result.status_code == 202, result.text
            receipt = result.json()
            assert [item["state"] for item in receipt["delivery"]["receipts"]] == [
                "accepted",
                "delivered",
            ]
            assert (await client.get(f"{base}/inspection")).json()["lifecycle"] == "running"
            replay = await client.post(f"{base}/commands", json=intent.model_dump(mode="json"))
            assert replay.status_code == 200, replay.text
            assert replay.json()["replay"] is True
            changed = intent.model_copy(update={"expected_generation": 2})
            assert (
                await client.post(f"{base}/commands", json=changed.model_dump(mode="json"))
            ).status_code == 409

            applied = ApplyBoundaryCommandAction(
                command_id=str(intent.request_id),
                command_issuer=receipt["admission"]["idempotency_issuer"],
                action=pause(),
                boundary_ref=TARGET.family_workflow_id,
                runnable_work_remains=False,
            )
            family_actor = trusted_actor.model_copy(
                update={
                    "actor_id": "orchestration-authority",
                    "permissions": trusted_actor.permissions
                    | {"workflow_run.apply_boundary_command"},
                }
            )
            boundary = BoundaryCommandApplicationService(authority, family_actor)
            applied_result = await boundary.execute(
                request_scope=scope,
                run_id=run_id,
                command_id=str(uuid4()),
                idempotency_issuer="family",
                correlation_id=str(intent.request_id),
                action=applied.model_dump(mode="json"),
                reason="qualified boundary reached",
            )
            assert applied_result.status == "accepted"
            assert (await client.get(f"{base}/inspection")).json()["lifecycle"] == "paused"
            commands = await authority.list_boundary_commands(scope, run_id)
            assert len(commands) == 1
            assert [item.state.value for item in commands[0].receipts] == [
                "accepted",
                "delivered",
                "applied",
            ]

            principal = principal.model_copy(update={"application_id": "ai-engineer"})
            assert (await client.get(f"{base}/inspection")).status_code == 403
        async with pool.acquire() as connection, connection.transaction():
            assert await connection.fetchval("SELECT current_user") == "belllabs_control_runtime"
            await connection.execute("SELECT set_config('belllabs.request_scope', $1, true)", scope)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.workflow_runs WHERE run_id=$1", run_id
                )
                == 1
            )
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'other-tenant', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.workflow_runs WHERE run_id=$1", run_id
                )
                == 0
            )
    finally:
        await pool.close()
