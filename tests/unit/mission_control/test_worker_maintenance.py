"""FT-C1 wiring: the worker serves `frames.expire` and schedules it per tenant scope."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from temporalio.service import RPCError, RPCStatusCode

from mission_control.adapters.auth.jwt import ActorGrant, ApplicationAuthentication
from mission_control.adapters.temporal.activities.frames_expire import FRAMES_EXPIRE_WORKFLOW
from mission_control.application.installations.registry import ApplicationBinding
from mission_control.bootstrap.api import ApplicationDeployment
from mission_control.bootstrap.worker import (
    ensure_retention_schedule,
    maintenance_task_queue,
    tenant_request_scopes,
)

INSTALLATION = UUID("0192a4f0-0000-7000-8000-00000000b10e")
TENANT_A = UUID("22222222-2222-4222-8222-222222222222")
TENANT_B = UUID("11111111-1111-4111-8111-111111111111")


def _application(tmp_path: Any, *grants: ActorGrant) -> ApplicationDeployment:
    binding = ApplicationBinding.seal(
        application_id="biotech",
        installation_id=INSTALLATION,
        binding_version="1",
        supabase_project_ref="local-test",
        database_secret_ref="MC_TEST_DSN",
        accepted_issuers={"https://issuer.invalid"},
        accepted_audiences={"authenticated"},
        required_component_version="1.1.0",
    )
    return ApplicationDeployment(
        authentication=ApplicationAuthentication(
            binding=binding,
            issuer="https://issuer.invalid",
            audience="authenticated",
            public_jwks_file=tmp_path / "public.json",
            grants=grants,
        )
    )


def _grant(*tenants: UUID) -> ActorGrant:
    return ActorGrant(
        subject=f"subject-{uuid4()}",
        actor_id="operator",
        tenant_ids=set(tenants),
        permissions={"workflow_run.read"},
    )


def test_retention_runs_on_every_granted_tenant_scope_once(tmp_path: Any) -> None:
    application = _application(tmp_path, _grant(TENANT_A), _grant(TENANT_A, TENANT_B))
    assert tenant_request_scopes(application) == [
        f"mc/{INSTALLATION}/biotech/{TENANT_B}",
        f"mc/{INSTALLATION}/biotech/{TENANT_A}",
    ]
    assert maintenance_task_queue("belllabs-biotech") == "belllabs-biotech-maintenance"


class _Schedules:
    def __init__(self, error: Exception | None = None) -> None:
        self.created: list[tuple[str, Any]] = []
        self._error = error

    async def create_schedule(self, schedule_id: str, schedule: Any) -> None:
        if self._error is not None:
            raise self._error
        self.created.append((schedule_id, schedule))


@pytest.mark.asyncio
async def test_retention_schedule_targets_the_maintenance_queue() -> None:
    client = _Schedules()
    schedule_id = await ensure_retention_schedule(
        client,  # type: ignore[arg-type]
        application_id="biotech",
        request_scopes=["mc/i/biotech/t"],
        task_queue="q-maintenance",
    )
    assert schedule_id == "mc-frames-expire:biotech"
    ((_, schedule),) = client.created
    assert schedule.action.workflow == FRAMES_EXPIRE_WORKFLOW
    assert schedule.action.task_queue == "q-maintenance"


@pytest.mark.asyncio
async def test_a_refused_schedule_is_reported_without_stopping_the_worker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    refused = _Schedules(RPCError("denied", RPCStatusCode.PERMISSION_DENIED, b""))
    assert (
        await ensure_retention_schedule(
            refused,  # type: ignore[arg-type]
            application_id="biotech",
            request_scopes=["mc/i/biotech/t"],
            task_queue="q",
        )
        is None
    )
    assert "will not expire" in caplog.text
    empty = _Schedules()
    assert (
        await ensure_retention_schedule(
            empty,  # type: ignore[arg-type]
            application_id="biotech",
            request_scopes=[],
            task_queue="q",
        )
        is None
    )
    assert not empty.created
