"""Worker startup proof against the installed common component and restricted roles."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

from mission_control.adapters.auth.jwt import ApplicationAuthentication
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.adapters.temporal.deployment_composition import (
    ProductionWorkerActivityCompositionFactory,
)
from mission_control.adapters.temporal.search_attributes import register_belllabs_search_attributes
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    InstallationUnavailable,
)
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    RuntimeOptions,
    TemporalDeployment,
)
from mission_control.bootstrap.worker import prepare_worker, run_worker, select_application
from mission_control.domain.authoring.extensions import ExtensionRegistry
from tests.fixtures.isolated_settings import isolated_settings
from tests.fixtures.mission_control_common_db import (
    COMPONENT_VERSION,
    common_database,
    provision_runtime,
)
from tests.fixtures.mission_control_production_stack import start_local

pytestmark = pytest.mark.common_db


@pytest.mark.asyncio
@pytest.mark.parametrize("poll_workers", [False, True])
async def test_worker_rechecks_actual_installation_on_restart_without_startup_mutation(
    monkeypatch,
    tmp_path,
    poll_workers,
):
    async with common_database() as database:
        checkpoint_dsn = await provision_runtime(database)
        monkeypatch.setenv("MC_WORKER_RUNTIME_DSN", database.dsn("mission_control_runtime"))
        monkeypatch.setenv("MC_WORKER_FAMILY_DSN", database.dsn("mission_control_family_writer"))
        await _worker_proof(database, checkpoint_dsn, tmp_path, poll_workers)


async def _worker_proof(database, checkpoint_dsn, tmp_path, poll_workers) -> None:
    binding = ApplicationBinding.seal(
        application_id="biotech",
        installation_id=database.installation_id,
        binding_version="worker-test-v1",
        supabase_project_ref=database.project_ref,
        database_secret_ref="MC_WORKER_RUNTIME_DSN",
        accepted_issuers={"https://worker.invalid"},
        accepted_audiences={"worker"},
        required_component_version=COMPONENT_VERSION,
    )
    authentication = ApplicationAuthentication(
        binding=binding,
        issuer="https://worker.invalid",
        audience="worker",
        public_jwks_file=tmp_path / "unused-worker-jwks.json",
        grants=(),
    )
    item = ApplicationDeployment(
        authentication=authentication,
        family_writer_secret_ref="MC_WORKER_FAMILY_DSN",
        temporal=TemporalDeployment(
            address="127.0.0.1:17233",
            namespace="worker-test",
            root_task_queue="worker-root",
            stagegraph_task_queue="worker-coordinator-family-stagegraph",
            goal_directed_task_queue="worker-coordinator-family-goal-directed",
        ),
    )
    deployment = MissionDeployment(
        storage_mode="production_common",
        applications=(item,),
        max_request_bytes=100_000,
    )
    settings = isolated_settings(
        coordinator_launch_enabled=True,
        langgraph_checkpoint_database_direct=SecretStr(checkpoint_dsn),
        langgraph_checkpoint_schema="mission_control_runtime",
        langgraph_checkpoint_setup=False,
    )
    inputs = {
        "application_id": "biotech",
        "binding_digest": binding.binding_digest,
        "settings": settings,
    }
    for _ in range(2):
        async with prepare_worker(deployment, **inputs) as prepared:
            assert prepared.readiness.database_name == database.name
            assert prepared.readiness.storage_mode == "production_common"
            assert prepared.readiness.pool_role == database.login_roles["mission_control_runtime"]
            assert prepared.settings.temporal_task_queue == "worker"
            assert prepared.settings.mission_control_catalog_scope == (
                f"mc/{binding.installation_id}/biotech/catalog"
            )
            assert not prepared.readiness.production_ready
    with pytest.raises(InstallationUnavailable, match="pin mismatch"):
        select_application(deployment, "biotech", "sha256:" + "0" * 64)
    wrong = ApplicationBinding.seal(
        **{
            **binding.model_dump(exclude={"binding_digest"}),
            "installation_id": uuid4(),
        }
    )
    wrong_deployment = deployment.model_copy(
        update={
            "applications": (
                item.model_copy(
                    update={"authentication": authentication.model_copy(update={"binding": wrong})}
                ),
            )
        }
    )
    with pytest.raises(InstallationUnavailable, match="differs from binding"):
        async with prepare_worker(
            wrong_deployment, **{**inputs, "binding_digest": wrong.binding_digest}
        ):
            pytest.fail("worker must reject a different persisted installation")
    with pytest.raises(InstallationUnavailable, match="checkpoint"):
        async with prepare_worker(
            deployment,
            **{
                **inputs,
                "settings": settings.model_copy(
                    update={
                        "langgraph_checkpoint_database_direct": SecretStr(
                            database.dsn("mission_control_runtime")
                            + "?options=-c%20search_path%3Dmission_control_runtime%2Cpg_temp"
                        )
                    },
                ),
            },
        ):
            pytest.fail("business writer cannot serve as checkpoint identity")
    with pytest.raises(InstallationUnavailable, match="operator step"):
        async with prepare_worker(
            deployment,
            **{
                **inputs,
                "settings": settings.model_copy(
                    update={"langgraph_checkpoint_setup": True},
                ),
            },
        ):
            pytest.fail("startup must never initialize checkpoint schema")
    if poll_workers:
        temporal = await start_local(tmp_path / "worker-temporal.sqlite", port=7346)
        stop = asyncio.Event()
        task = None
        try:
            await register_belllabs_search_attributes(temporal.client, "default")
            polling_deployment = deployment.model_copy(
                update={
                    "applications": (
                        item.model_copy(
                            update={
                                "temporal": item.temporal.model_copy(
                                    update={
                                        "address": "127.0.0.1:7346",
                                        "namespace": "default",
                                    }
                                )
                            }
                        ),
                    )
                }
            )
            polling_settings = settings.model_copy(
                update={
                    "capability_bundle_backend": "local",
                    "web_research_agent_browser_node": Path(shutil.which("node")),
                    "artifact_payload_root": tmp_path / "artifacts",
                    "deep_agent_sandbox_workspace_root": tmp_path / "workspaces",
                }
            )
            options = RuntimeOptions(
                AdmissionPolicyRegistry(),
                ExtensionRegistry(),
                UnavailablePayloadStore(),
                FamilyAdmissionRegistry(),
            )

            class CheckedFactory(ProductionWorkerActivityCompositionFactory):
                async def build(self, **kwargs):
                    catalog = kwargs["control_plane"]
                    runs = kwargs["run_control"]
                    assert catalog._extensions is options.extensions
                    assert catalog._payload_store is options.payload_store
                    assert runs._policies is options.admission_policies
                    assert runs._family_admissions is options.family_admissions
                    return await super().build(**kwargs)

            task = asyncio.create_task(
                run_worker(
                    polling_deployment,
                    **{**inputs, "settings": polling_settings},
                    stop=stop,
                    runtime_options=options,
                    composition_factory=CheckedFactory(temporal.client),
                )
            )
            # Inspect the real server: all family, root and linked queues must
            # have active pollers from the actual entrypoint's worker classes.
            for queue in (
                "worker-root",
                "worker-coordinator-family-stagegraph",
                "worker-coordinator-family-goal-directed",
                "worker-linked-runs",
            ):
                async with asyncio.timeout(60):
                    while True:
                        if task.done():
                            await task
                            pytest.fail("worker exited before polling")
                        result = await temporal.client.workflow_service.describe_task_queue(
                            DescribeTaskQueueRequest(
                                namespace="default",
                                task_queue=TaskQueue(name=queue),
                                task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                            )
                        )
                        if result.pollers:
                            break
                        await asyncio.sleep(0.1)
        finally:
            stop.set()
            try:
                if task is not None:
                    await asyncio.wait_for(task, timeout=30)
            finally:
                await temporal.shutdown()
